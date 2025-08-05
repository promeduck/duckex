// SPDX-FileCopyrightText: 2025 Stas Muzhyk <sts@abc3.dev>
// SPDX-FileCopyrightText: 2025 Łukasz Niemier <~@hauleth.dev>
//
// SPDX-License-Identifier: Apache-2.0

pub(crate) struct Cache<T> {
    storage: Vec<Option<T>>,
    idx: usize,
}

impl<T> Default for Cache<T> {
    fn default() -> Self {
        Self::with_capacity(1024)
    }
}

impl<T> Cache<T> {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        let mut storage = vec![];
        storage.resize_with(capacity, Default::default);

        Cache { storage, idx: 0 }
    }

    pub(crate) fn store(&mut self, data: T) -> Option<u32> {
        let mut idx = self.idx;

        // Find first unoccupied entry
        while let Some(_) = self.storage[idx] {
            idx += 1;
            idx %= self.storage.len();

            if idx == self.idx {
                return None;
            }
        }

        self.storage[idx] = Some(data);

        Some(idx as u32)
    }

    pub(crate) fn remove(&mut self, idx: usize) {
        let _ = self.storage[idx].take();
    }

    pub(crate) fn get_mut(&mut self, idx: usize) -> Option<&mut T> {
        self.storage[idx].as_mut()
    }
}

impl<T> std::ops::Index<usize> for Cache<T> {
    type Output = Option<T>;

    fn index(&self, idx: usize) -> &Option<T> {
        &self.storage[idx]
    }
}

impl<T> std::ops::IndexMut<usize> for Cache<T> {
    fn index_mut(&mut self, idx: usize) -> &mut Option<T> {
        &mut self.storage[idx]
    }
}
