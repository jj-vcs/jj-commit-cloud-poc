const EMPTY_STRING_PLACEHOLDER: &str = "JJ_EMPTY_STRING";

/// Appends a byte slice preceded by its 8-byte length prefix to prevent boundary hash collisions (e.g. "foo"+"bar" vs "foob"+"ar").
// Matches length-prefixed hashing used internally at Google
fn append_length_prefixed_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// Appends a UTF-8 string slice preceded by its length prefix.
fn append_length_prefixed_str(buf: &mut Vec<u8>, s: &str) {
    append_length_prefixed_bytes(buf, s.as_bytes());
}

fn signature_to_git(sig: Option<&cc_common::backend::Signature>) -> gix::actor::Signature {
    let (name, email) = match sig {
        Some(s) => (
            if s.name.is_empty() {
                EMPTY_STRING_PLACEHOLDER
            } else {
                &s.name
            },
            if s.email.is_empty() {
                EMPTY_STRING_PLACEHOLDER
            } else {
                &s.email
            },
        ),
        None => (EMPTY_STRING_PLACEHOLDER, EMPTY_STRING_PLACEHOLDER),
    };
    let (secs, offset_mins) = sig
        .and_then(|s| s.timestamp.as_ref())
        .map_or((0, 0), |t| (t.millis_since_epoch / 1000, t.tz_offset));

    gix::actor::Signature {
        name: name.into(),
        email: email.into(),
        time: gix::date::Time::new(secs, offset_mins * 60),
    }
}

const REVERSE_HEX_CHARS: &[u8; 16] = b"zyxwvutsrqponmlk";
const FORWARD_HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

fn encode_reverse_hex(data: &[u8]) -> String {
    let encoded: Vec<u8> = data
        .iter()
        .flat_map(|b| {
            [
                REVERSE_HEX_CHARS[usize::from(b >> 4)],
                REVERSE_HEX_CHARS[usize::from(b & 0x0f)],
            ]
        })
        .collect();
    String::from_utf8(encoded).expect("ASCII z-k digits must be valid UTF-8")
}

fn encode_forward_hex(data: &[u8]) -> String {
    let encoded: Vec<u8> = data
        .iter()
        .flat_map(|b| {
            [
                FORWARD_HEX_CHARS[usize::from(b >> 4)],
                FORWARD_HEX_CHARS[usize::from(b & 0x0f)],
            ]
        })
        .collect();
    String::from_utf8(encoded).expect("ASCII hex digits must be valid UTF-8")
}

/// Standalone black-box function that computes a Git commit hash using `gix` (Gitoxide),
/// matching `jj_lib::git_backend::GitBackend` commit serialization bit-for-bit.
pub fn compute_git_commit_hash(commit: &cc_common::backend::Commit) -> Vec<u8> {
    use gix::objs::WriteTo;

    let tree_id = commit
        .root_tree_id
        .first()
        .and_then(|id| gix::hash::ObjectId::try_from(id.as_slice()).ok())
        .unwrap_or_else(|| gix::hash::ObjectId::empty_tree(gix::hash::Kind::Sha1));

    // Exclude Jujutsu's root commit ID (Git root commits have 0 parents in Git representation)
    let parents: Vec<gix::hash::ObjectId> = commit
        .parent_commit_ids
        .iter()
        .filter(|id| id.as_slice() != cc_common::ROOT_COMMIT_ID_BYTES)
        .filter_map(|id| gix::hash::ObjectId::try_from(id.as_slice()).ok())
        .collect();

    let mut extra_headers = Vec::new();

    if commit.conflict_labels.len() > 1 {
        let mut joined = commit.conflict_labels.join("\n");
        joined.push('\n');
        extra_headers.push(("jj:conflict-labels".into(), joined.into()));
    }

    if commit.root_tree_id.len() > 1 {
        let trees_hex: Vec<String> = commit
            .root_tree_id
            .iter()
            .map(|id| encode_forward_hex(id))
            .collect();
        extra_headers.push(("jj:trees".into(), trees_hex.join(" ").into()));
    }

    if !commit.change_id.is_empty() {
        let rev_hex = encode_reverse_hex(&commit.change_id);
        extra_headers.push(("change-id".into(), rev_hex.into()));
    }

    if let Some(secure_sig) = &commit.secure_sig {
        if !secure_sig.sig.is_empty() {
            extra_headers.push(("gpgsig".into(), secure_sig.sig.clone().into()));
        }
    }

    let gix_commit = gix::objs::Commit {
        tree: tree_id,
        parents: parents.into(),
        author: signature_to_git(commit.author.as_ref()),
        committer: signature_to_git(commit.committer.as_ref()),
        encoding: None,
        extra_headers,
        message: commit.description.as_bytes().into(),
    };

    let mut buf = Vec::new();
    gix_commit
        .write_to(&mut buf)
        .expect("gix commit should have serialized successfully");
    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, &buf);
    hash.as_bytes().to_vec()
}

/// Standalone black-box function that computes a Git blob hash using `gix` (Gitoxide).
pub fn compute_git_blob_hash(content: &[u8]) -> Vec<u8> {
    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, content);
    hash.as_bytes().to_vec()
}

/// Standalone black-box function that computes a Git tree hash using `gix` (Gitoxide).
pub fn compute_git_tree_hash(entries: &[cc_common::backend::TreeEntry]) -> Vec<u8> {
    use gix::objs::WriteTo;

    let mut gix_entries = Vec::new();
    for entry in entries {
        let (kind, id_bytes) = match entry.value.as_ref().and_then(|v| v.value.as_ref()) {
            Some(cc_common::backend::tree_value::Value::File(f)) => {
                let k = if f.executable {
                    gix::objs::tree::EntryKind::BlobExecutable
                } else {
                    gix::objs::tree::EntryKind::Blob
                };
                (k, &f.id[..])
            }
            Some(cc_common::backend::tree_value::Value::TreeId(id)) => {
                (gix::objs::tree::EntryKind::Tree, &id[..])
            }
            Some(cc_common::backend::tree_value::Value::SymlinkId(id)) => {
                (gix::objs::tree::EntryKind::Link, &id[..])
            }
            _ => (
                gix::objs::tree::EntryKind::Blob,
                &cc_common::ROOT_COMMIT_ID_BYTES[..],
            ),
        };
        if let Ok(oid) = gix::hash::ObjectId::try_from(id_bytes) {
            gix_entries.push(gix::objs::tree::Entry {
                mode: kind.into(),
                filename: entry.name.as_bytes().into(),
                oid,
            });
        }
    }

    // Sort entries according to Git canonical tree entry ordering rules (gix::objs::tree::Entry implements Ord for Git tree sorting).
    gix_entries.sort_unstable();

    let gix_tree = gix::objs::Tree {
        entries: gix_entries,
    };
    let mut buf = Vec::new();
    gix_tree
        .write_to(&mut buf)
        .expect("gix tree should have serialized successfully");

    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Tree, &buf);
    hash.as_bytes().to_vec()
}

// Both view id and operation id keys must be sorted to get deterministic hashing because map key iteration order is not guaranteed by rust, can change etc and we need deterministic hashing for content addressed storage.
pub fn hash_operation(op: &cc_common::op_store::Operation) -> Vec<u8> {
    let mut buf = Vec::new();
    append_length_prefixed_bytes(&mut buf, &op.view_id);
    for p in &op.parents {
        append_length_prefixed_bytes(&mut buf, p);
    }
    if let Some(meta) = &op.metadata {
        buf.extend_from_slice(&meta.start_time_millis.to_le_bytes());
        buf.extend_from_slice(&meta.end_time_millis.to_le_bytes());
        append_length_prefixed_str(&mut buf, &meta.description);
        append_length_prefixed_str(&mut buf, &meta.hostname);
        append_length_prefixed_str(&mut buf, &meta.username);
        buf.extend_from_slice(&(meta.is_snapshot as u8).to_le_bytes());
        if let Some(ws) = &meta.workspace_name {
            append_length_prefixed_str(&mut buf, ws);
        }
        let sorted_attrs: std::collections::BTreeMap<_, _> = meta.attributes.iter().collect();
        for (k, v) in sorted_attrs {
            append_length_prefixed_str(&mut buf, k);
            append_length_prefixed_str(&mut buf, v);
        }
    }
    for pred in &op.commit_predecessors {
        append_length_prefixed_bytes(&mut buf, &pred.commit_id);
        for p_id in &pred.predecessor_ids {
            append_length_prefixed_bytes(&mut buf, p_id);
        }
    }
    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, &buf);
    hash.as_bytes()[..cc_common::OPERATION_ID_LENGTH].to_vec()
}

/// Computes a unique SHA-1 ViewId hash.
pub fn hash_view(view: &cc_common::op_store::View) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut head_ids = view.head_ids.clone();
    head_ids.sort();
    for head in &head_ids {
        append_length_prefixed_bytes(&mut buf, head);
    }

    let sorted_wc: std::collections::BTreeMap<_, _> = view.wc_commit_ids.iter().collect();
    for (k, v) in sorted_wc {
        append_length_prefixed_str(&mut buf, k);
        append_length_prefixed_bytes(&mut buf, v);
    }

    let append_ref_target = |buf: &mut Vec<u8>, target: &cc_common::op_store::RefTarget| {
        let mut removes: Vec<_> = target.removes.iter().map(|t| &t.commit_id).collect();
        removes.sort();
        for commit_id in removes {
            append_length_prefixed_bytes(buf, commit_id);
        }
        let mut adds: Vec<_> = target.adds.iter().map(|t| &t.commit_id).collect();
        adds.sort();
        for commit_id in adds {
            append_length_prefixed_bytes(buf, commit_id);
        }
    };

    let sorted_bookmarks: std::collections::BTreeMap<_, _> =
        view.local_bookmarks.iter().collect();
    for (name, target) in sorted_bookmarks {
        append_length_prefixed_str(&mut buf, name);
        append_ref_target(&mut buf, target);
    }

    let sorted_remotes: std::collections::BTreeMap<_, _> =
        view.remote_bookmarks.iter().collect();
    for (name, remote_ref) in sorted_remotes {
        append_length_prefixed_str(&mut buf, name);
        buf.extend_from_slice(&(remote_ref.is_tracked as u8).to_le_bytes());
        if let Some(target) = &remote_ref.target {
            append_ref_target(&mut buf, target);
        }
    }

    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, &buf);
    hash.as_bytes()[..cc_common::VIEW_ID_LENGTH].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_reverse_hex_matches_jj_alphabet() {
        assert_eq!(
            encode_reverse_hex(b"\x01\x23\x45\x67\x89\xab\xcd\xef"),
            "zyxwvutsrqponmlk"
        );
    }

    #[test]
    fn test_compute_git_commit_hash_with_secure_sig_and_change_id() {
        let mut commit = cc_common::backend::Commit {
            commit_id: vec![],
            change_id: b"\x01\x23\x45\x67\x89\xab\xcd\xef\x01\x23\x45\x67\x89\xab\xcd\xef".to_vec(),
            parent_commit_ids: vec![],
            root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            description: "test commit\n".to_string(),
            author: None,
            committer: None,
            predecessors: vec![],
            conflict_labels: vec![],
            secure_sig: None,
        };
        let hash_unsigned = compute_git_commit_hash(&commit);
        assert_eq!(hash_unsigned.len(), 20);

        commit.secure_sig = Some(cc_common::backend::SecureSig {
            data: b"unsigned payload".to_vec(),
            sig: b"-----BEGIN PGP SIGNATURE-----\nfake-sig\n-----END PGP SIGNATURE-----\n".to_vec(),
        });
        let hash_signed = compute_git_commit_hash(&commit);
        assert_eq!(hash_signed.len(), 20);
        assert_ne!(hash_unsigned, hash_signed);
    }
}
