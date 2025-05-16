// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

///
/// Minimal, very fast basic random num gen, suitable when enough is to have something random (no crypto suitable etc)
/// https://en.wikipedia.org/wiki/Permuted_congruential_generator
///
pub struct FastRand {
    state: u64,
    increment: u64,
}

impl FastRand {
    pub fn new(seed: u64) -> Self {
        Self {
            state: seed,
            increment: 1442695040888963407,
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> u32 {
        let old_state = self.state;
        self.state = old_state.wrapping_mul(6364136223846793005).wrapping_add(self.increment);
        let xor_shifted = ((old_state >> 18) ^ old_state) >> 27;
        let rot = (old_state >> 59) as u32;
        (xor_shifted.rotate_right(rot) & 0xFFFFFFFF) as u32
    }
}
