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

use super::tag::Tag;
use iceoryx2_bb_container::slotmap::SlotMapKey;

#[derive(Debug, PartialEq)]
#[allow(dead_code)]
#[allow(clippy::enum_variant_names)]
/// MapIdentifier is an enum that represents the type of SlotMap the orchestration tag belongs to.
pub(crate) enum MapIdentifier {
    InvokeMap,
    EventMap,
    SimpleConditionMap,
    ComplexConditionMap,
}

#[derive(Debug, PartialEq)]
/// OrchTagId is a struct that contains the tag ID, a key for SlotMap, and a SlotMap identifier.
pub(crate) struct OrchTagId {
    tag: Tag,
    key: SlotMapKey,
    map_identifier: MapIdentifier,
}

/// OrchestrationTag is a wrapper around OrchTagId that provides a convenient way to create and manage orchestration tags.
/// It contains tag ID, a key for SlotMap, and a SlotMap identifier.
/// The tag ID is used to uniquely identify the orchestration tag.
#[derive(Debug, PartialEq)]
pub struct OrchestrationTag {
    id: OrchTagId,
    // May have to add more fields in the future
}

#[allow(dead_code)]
impl OrchestrationTag {
    /// Create a new orchestration tag with the given Tag, SlotMapKey, and MapIdentifier.
    pub(crate) fn new(tag: Tag, key: SlotMapKey, map_identifier: MapIdentifier) -> Self {
        Self {
            id: OrchTagId { tag, key, map_identifier },
        }
    }

    /// Get the tag ID of the orchestration tag.
    #[inline]
    pub(crate) fn id(&self) -> Tag {
        self.id.tag
    }

    /// Get the SlotMapKey of the orchestration tag.
    #[inline]
    pub(crate) fn key(&self) -> &SlotMapKey {
        &self.id.key
    }

    /// Get the SlotMap identifier of the orchestration tag.
    #[inline]
    pub(crate) fn map_identifier(&self) -> &MapIdentifier {
        &self.id.map_identifier
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceoryx2_bb_container::slotmap::SlotMapKey;

    #[test]
    fn test_orchestration_tag_creation() {
        let tag = OrchestrationTag::new(Tag::from_str_static("test_tag"), SlotMapKey::new(1), MapIdentifier::InvokeMap);
        assert_eq!(tag.id(), Tag::from_str_static("test_tag"));
        assert_eq!(*tag.key(), SlotMapKey::new(1));
        assert_eq!(*tag.map_identifier(), MapIdentifier::InvokeMap);
    }

    #[test]
    fn test_orchestration_tag_equality() {
        let tag1 = OrchestrationTag::new(Tag::from_str_static("test_tag"), SlotMapKey::new(2), MapIdentifier::EventMap);
        let tag2 = OrchestrationTag::new(Tag::from_str_static("test_tag"), SlotMapKey::new(2), MapIdentifier::EventMap);

        // Equality is based on `id.tag`, `id.key` and `id.map_identifier`, these should be equal
        assert_eq!(tag1, tag2);

        let tag3 = OrchestrationTag::new(Tag::from_str_static("different_tag"), SlotMapKey::new(2), MapIdentifier::EventMap);
        // These should not be equal because `id.tag` is different
        assert_ne!(tag1, tag3);

        let tag4 = OrchestrationTag::new(Tag::from_str_static("different_tag"), SlotMapKey::new(1), MapIdentifier::EventMap);
        // These should not be equal because `id.key` is different
        assert_ne!(tag3, tag4);

        let tag5 = OrchestrationTag::new(
            Tag::from_str_static("different_tag"),
            SlotMapKey::new(1),
            MapIdentifier::SimpleConditionMap,
        );
        // These should not be equal because `id.map_identifier` is different
        assert_ne!(tag4, tag5);
    }
}
