use jj_lib::backend::*;
use jj_lib::object_id::ObjectId;
use jj_lib::repo_path::RepoPathComponentBuf;

use crate::backend as pb;

pub fn signature_to_proto(sig: &Signature) -> pb::Signature {
    pb::Signature {
        name: sig.name.clone(),
        email: sig.email.clone(),
        timestamp: Some(pb::Timestamp {
            millis_since_epoch: sig.timestamp.timestamp.0,
            tz_offset: sig.timestamp.tz_offset,
        }),
    }
}

pub fn signature_from_proto(proto: pb::Signature) -> Signature {
    let ts = proto.timestamp.unwrap_or_default();
    Signature {
        name: proto.name,
        email: proto.email,
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(ts.millis_since_epoch),
            tz_offset: ts.tz_offset,
        },
    }
}

pub fn commit_to_proto(commit: &Commit) -> pb::Commit {
    pb::Commit {
        commit_id: vec![],
        change_id: commit.change_id.to_bytes().to_vec(),
        parent_commit_ids: commit
            .parents
            .iter()
            .map(|id| id.to_bytes().to_vec())
            .collect(),
        root_tree_id: commit
            .root_tree
            .iter()
            .map(|id| id.to_bytes().to_vec())
            .collect(),
        description: commit.description.clone(),
        author: Some(signature_to_proto(&commit.author)),
        committer: Some(signature_to_proto(&commit.committer)),
        predecessors: commit
            .predecessors
            .iter()
            .map(|id| id.to_bytes().to_vec())
            .collect(),
        conflict_labels: commit.conflict_labels.as_slice().to_owned(),
        secure_sig: commit.secure_sig.as_ref().map(|s| s.sig.clone()),
    }
}

pub fn commit_from_proto(proto_commit: pb::Commit) -> Commit {
    let author = signature_from_proto(proto_commit.author.unwrap_or_default());
    let committer = signature_from_proto(proto_commit.committer.unwrap_or_default());

    let merge_builder: jj_lib::merge::MergeBuilder<_> = proto_commit
        .root_tree_id
        .into_iter()
        .map(|b| TreeId::from_bytes(&b))
        .collect();

    let root_tree = merge_builder.build();
    let conflict_labels =
        jj_lib::conflict_labels::ConflictLabels::from_vec(proto_commit.conflict_labels)
            .into_merge();

    Commit {
        parents: proto_commit
            .parent_commit_ids
            .iter()
            .map(|b| CommitId::from_bytes(b))
            .collect(),
        predecessors: proto_commit
            .predecessors
            .iter()
            .map(|b| CommitId::from_bytes(b))
            .collect(),
        root_tree,
        change_id: ChangeId::from_bytes(&proto_commit.change_id),
        description: proto_commit.description,
        author,
        committer,
        conflict_labels,
        secure_sig: None,
    }
}

pub fn tree_entry_to_proto(entry: &jj_lib::backend::TreeEntry) -> BackendResult<pb::TreeEntry> {
    let value = match entry.value() {
        TreeValue::File {
            id,
            executable,
            copy_id,
        } => pb::TreeValue {
            value: Some(pb::tree_value::Value::File(pb::File {
                id: id.to_bytes().to_vec(),
                executable: *executable,
                copy_id: copy_id.to_bytes().to_vec(),
            })),
        },
        TreeValue::Symlink(id) => pb::TreeValue {
            value: Some(pb::tree_value::Value::SymlinkId(id.to_bytes().to_vec())),
        },
        TreeValue::Tree(id) => pb::TreeValue {
            value: Some(pb::tree_value::Value::TreeId(id.to_bytes().to_vec())),
        },
        TreeValue::GitSubmodule(_id) => {
            return Err(BackendError::Unsupported(
                "Git submodules are not supported".into(),
            ));
        }
    };

    Ok(pb::TreeEntry {
        name: entry.name().as_internal_str().to_string(),
        value: Some(value),
    })
}

pub fn tree_entry_from_proto(
    proto_entry: pb::TreeEntry,
) -> Result<(RepoPathComponentBuf, TreeValue), Box<dyn std::error::Error + Send + Sync>> {
    let component = RepoPathComponentBuf::new(proto_entry.name)?;
    let proto_val = proto_entry
        .value
        .ok_or_else(|| "tree entry should have contained a TreeValue")?;
    let val = match proto_val
        .value
        .ok_or_else(|| "TreeValue should have contained an inner value")?
    {
        pb::tree_value::Value::File(f) => TreeValue::File {
            id: FileId::from_bytes(&f.id),
            executable: f.executable,
            copy_id: CopyId::from_bytes(if f.copy_id.len() == crate::COMMIT_ID_LENGTH {
                &f.copy_id
            } else {
                &crate::ROOT_COMMIT_ID_BYTES
            }),
        },
        pb::tree_value::Value::SymlinkId(id) => TreeValue::Symlink(SymlinkId::from_bytes(&id)),
        pb::tree_value::Value::TreeId(id) => TreeValue::Tree(TreeId::from_bytes(&id)),
        pb::tree_value::Value::ConflictId(_) => {
            return Err("tree entry should not have contained a ConflictId".into());
        }
    };
    Ok((component, val))
}
