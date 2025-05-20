//
// Copyright (c) 2025 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache License Version 2.0 which is available at
// <https://www.apache.org/licenses/LICENSE-2.0>
//
// SPDX-License-Identifier: Apache-2.0
//

use std::fmt::{Debug, Formatter};

///
/// Tag implementation with an 'id' and 'tracing info string'. Supports creation of Tag from &str and String, also from_str_ref().
///
#[derive(Clone, Copy)]
pub struct Tag {
    id: u64,
    tracing_str: &'static str,
}

impl Tag {
    /// Create Tag from static string.
    pub fn from_str_static(s: &'static str) -> Self {
        Self {
            id: Tag::compute_djb2_hash(s),
            tracing_str: s,
        }
    }

    /// ID of the Tag.
    #[inline]
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Tracing info string of the Tag. It is empty if 'orch_tracing' is not enabled during compilation.
    #[inline]
    pub fn tracing_str(&self) -> &str {
        self.tracing_str
    }

    // Function to calculate hash of string to use it as Tag id.
    const fn compute_djb2_hash(s: &str) -> u64 {
        let bytes = s.as_bytes();
        let mut hash: u64 = 5381;
        let mut i = 0;

        while i < bytes.len() {
            hash = ((hash << 5).wrapping_add(hash)).wrapping_add(bytes[i] as u64); // hash * 33 + c
            i += 1;
        }

        hash
    }
}

/// Create Tag from &str.
#[allow(clippy::from_over_into)]
impl Into<Tag> for &str {
    fn into(self) -> Tag {
        Tag {
            id: Tag::compute_djb2_hash(self),
            #[cfg(orch_tracing)]
            tracing_str: self.to_owned().leak(),
            #[cfg(not(orch_tracing))]
            tracing_str: "",
        }
    }
}

/// Create Tag from String.
#[allow(clippy::from_over_into)]
impl Into<Tag> for String {
    fn into(self) -> Tag {
        Tag {
            id: Tag::compute_djb2_hash(&self),
            #[cfg(orch_tracing)]
            tracing_str: self.leak(),
            #[cfg(not(orch_tracing))]
            tracing_str: "",
        }
    }
}

// Implementation of Debug fmt for Tag.
impl Debug for Tag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Tag{{id:{},tracing_str:\"{}\"}}", self.id, self.tracing_str)
    }
}

// Implementation of PartialEq for Tag to compare 'id'.
impl PartialEq for Tag {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_tracing_str(ins: &str, _expected: &str) {
        #[cfg(orch_tracing)]
        assert_eq!(ins, _expected);
        #[cfg(not(orch_tracing))]
        assert_eq!(ins, "");
    }

    #[test]
    fn test_tag_from_str() {
        let tag = Tag::from_str_static("test_info");
        let expected_id = Tag::compute_djb2_hash("test_info");

        assert_eq!(tag.id(), expected_id);
        assert_eq!(tag.tracing_str(), "test_info");
    }

    #[test]
    fn test_tag_from_static_str() {
        let tag: Tag = "static_tag".into();
        let expected_id = Tag::compute_djb2_hash("static_tag");

        assert_eq!(tag.id(), expected_id);
        assert_tracing_str(tag.tracing_str(), "static_tag");
    }

    #[test]
    fn test_tag_from_string() {
        let input = String::from("dynamic_tag");
        let expected_id = Tag::compute_djb2_hash(&input);

        let tag: Tag = input.clone().into();

        assert_eq!(tag.id(), expected_id);
        assert_tracing_str(tag.tracing_str(), "dynamic_tag");
    }

    #[test]
    fn test_compute_qorhash_known_value() {
        const EXPECTED_HASH: u64 = 229465095248369; // Precomputed for "example"
        let actual_hash = Tag::compute_djb2_hash("example");

        assert_eq!(actual_hash, EXPECTED_HASH);
    }

    #[test]
    fn test_consistent_hash_for_same_str() {
        let static_tag: Tag = "consistency".into();
        let string_tag: Tag = String::from("consistency").into();

        assert_eq!(static_tag.id, string_tag.id);
        assert_tracing_str(static_tag.tracing_str(), "consistency");
        assert_tracing_str(string_tag.tracing_str(), "consistency");
    }

    #[test]
    fn test_tag_comparision() {
        let tag1: Tag = "same_string".into();
        let tag2: Tag = String::from("same_string").into();
        let tag3: Tag = String::from("different_string").into();

        assert_eq!(tag1 == tag2, true);
        assert_eq!(tag1 == tag3, false);
    }
}
