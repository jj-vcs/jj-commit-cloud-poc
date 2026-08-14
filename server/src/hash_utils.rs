const EMPTY_STRING_PLACEHOLDER: &str = "JJ_EMPTY_STRING";

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

/// Standalone black-box function that computes a Git commit hash using `gix` (Gitoxide).
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
    if !commit.change_id.is_empty() {
        use std::fmt::Write;
        // multiply by 2 since 1 byte is 2 hex string characters
        let mut hex_str = String::with_capacity(commit.change_id.len() * 2);
        for b in commit.change_id.iter().rev() {
            let _ = write!(hex_str, "{:02x}", b);
        }
        extra_headers.push(("change-id".into(), hex_str.into()));
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
