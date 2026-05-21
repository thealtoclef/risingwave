// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! FIFO + TTL set of partition tokens, single-threaded.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

pub struct BoundedTokenCache {
    max_entries: usize,
    ttl: Duration,
    entries: HashMap<String, Instant>,
    insertion_order: VecDeque<String>,
}

impl BoundedTokenCache {
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            max_entries,
            ttl,
            entries: HashMap::new(),
            insertion_order: VecDeque::new(),
        }
    }

    pub fn contains(&mut self, token: &str) -> bool {
        self.prune_expired();
        self.entries.contains_key(token)
    }

    pub fn insert(&mut self, token: String) {
        self.prune_expired();
        if self.entries.contains_key(&token) {
            if let Some(pos) = self.insertion_order.iter().position(|t| t == &token) {
                self.insertion_order.remove(pos);
            }
            self.entries.insert(token.clone(), Instant::now());
            self.insertion_order.push_back(token);
            return;
        }
        if self.entries.len() >= self.max_entries
            && let Some(oldest) = self.insertion_order.pop_front()
        {
            self.entries.remove(&oldest);
        }
        self.entries.insert(token.clone(), Instant::now());
        self.insertion_order.push_back(token);
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    fn prune_expired(&mut self) {
        let now = Instant::now();
        while let Some(front) = self.insertion_order.front() {
            match self.entries.get(front) {
                Some(t) if now.duration_since(*t) > self.ttl => {
                    let token = self.insertion_order.pop_front().unwrap();
                    self.entries.remove(&token);
                }
                Some(_) => break,
                None => {
                    self.insertion_order.pop_front();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_contains() {
        let mut c = BoundedTokenCache::new(10, Duration::from_secs(60));
        c.insert("a".to_string());
        assert!(c.contains("a"));
        assert!(!c.contains("b"));
    }

    #[test]
    fn capacity_evicts_oldest() {
        let mut c = BoundedTokenCache::new(3, Duration::from_secs(60));
        c.insert("a".to_string());
        c.insert("b".to_string());
        c.insert("c".to_string());
        c.insert("d".to_string());
        assert_eq!(c.len(), 3);
        assert!(!c.contains("a"));
        assert!(c.contains("b"));
        assert!(c.contains("c"));
        assert!(c.contains("d"));
    }

    #[test]
    fn ttl_evicts_expired() {
        let mut c = BoundedTokenCache::new(10, Duration::from_millis(50));
        c.insert("a".to_string());
        assert!(c.contains("a"));
        std::thread::sleep(Duration::from_millis(80));
        assert!(!c.contains("a"));
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn reinsert_refreshes_position() {
        let mut c = BoundedTokenCache::new(3, Duration::from_secs(60));
        c.insert("a".to_string());
        c.insert("b".to_string());
        c.insert("c".to_string());
        c.insert("a".to_string());
        c.insert("d".to_string());
        assert!(c.contains("a"));
        assert!(!c.contains("b"));
        assert!(c.contains("c"));
        assert!(c.contains("d"));
    }
}
