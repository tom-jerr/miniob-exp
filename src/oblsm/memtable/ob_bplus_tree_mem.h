/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <shared_mutex>
#include <algorithm>
#include "common/lang/vector.h"
#include "common/lang/mutex.h"
#include "common/log/log.h"

namespace oceanbase {

/**
 * @class ObMemBPlusTree
 * @brief In-memory B+Tree container with the exact same interface contract as ObSkipList,
 *        allowing ObMemTable to seamlessly switch between SkipList and B+Tree while
 *        flushing to identical on-disk SSTable files.
 */
template <typename Key, class ObComparator>
class ObMemBPlusTree
{
private:
  static constexpr int kMaxKeys = 32;

  struct Node
  {
    bool is_leaf;
    explicit Node(bool leaf) : is_leaf(leaf) {}
    virtual ~Node() = default;
  };

  struct LeafNode : public Node
  {
    std::vector<Key> keys;
    LeafNode        *next = nullptr;
    LeafNode        *prev = nullptr;
    LeafNode() : Node(true) { keys.reserve(kMaxKeys + 1); }
  };

  struct InternalNode : public Node
  {
    std::vector<Key>    keys;
    std::vector<Node *> children;
    InternalNode() : Node(false)
    {
      keys.reserve(kMaxKeys + 1);
      children.reserve(kMaxKeys + 2);
    }
    ~InternalNode() override
    {
      for (Node *child : children) {
        delete child;
      }
    }
  };

public:
  explicit ObMemBPlusTree(ObComparator cmp) : compare_(cmp)
  {
    LeafNode *root_leaf = new LeafNode();
    root_               = root_leaf;
    first_leaf_         = root_leaf;
    last_leaf_          = root_leaf;
  }

  ObMemBPlusTree(const ObMemBPlusTree &)            = delete;
  ObMemBPlusTree &operator=(const ObMemBPlusTree &) = delete;

  ~ObMemBPlusTree()
  {
    // If root_ is an InternalNode, its destructor recursively deletes all children including leaves.
    // If root_ is a LeafNode, deleting root_ frees it directly.
    delete root_;
  }

  void insert(const Key &key)
  {
    std::unique_lock<std::shared_mutex> lock(rw_lock_);
    insert_internal(key);
  }

  void insert_concurrently(const Key &key)
  {
    std::unique_lock<std::shared_mutex> lock(rw_lock_);
    insert_internal(key);
  }

  bool contains(const Key &key) const
  {
    std::shared_lock<std::shared_mutex> lock(rw_lock_);
    LeafNode                           *leaf = find_leaf(key);
    if (leaf == nullptr || leaf->keys.empty()) {
      return false;
    }
    auto it = std::lower_bound(
        leaf->keys.begin(), leaf->keys.end(), key, [this](const Key &a, const Key &b) { return compare_(a, b) < 0; });
    return it != leaf->keys.end() && compare_(*it, key) == 0;
  }

  class Iterator
  {
  public:
    explicit Iterator(const ObMemBPlusTree *tree) : tree_(tree), leaf_(nullptr), idx_(0), valid_(false), current_key_{}
    {}

    bool valid() const { return valid_; }

    const Key &key() const
    {
      ASSERT(valid_, "valid");
      return current_key_;
    }

    void next()
    {
      std::shared_lock<std::shared_mutex> lock(tree_->rw_lock_);
      if (!valid_ || leaf_ == nullptr) {
        valid_ = false;
        return;
      }
      // Fast path if no concurrent shift happened in current leaf
      if (idx_ + 1 < static_cast<int>(leaf_->keys.size()) && tree_->compare_(leaf_->keys[idx_ + 1], current_key_) > 0 &&
          (idx_ < static_cast<int>(leaf_->keys.size()) && tree_->compare_(leaf_->keys[idx_], current_key_) == 0)) {
        idx_++;
        current_key_ = leaf_->keys[idx_];
        return;
      }
      // Concurrent insertion/split occurred: advance to first key strictly greater than current_key_
      LeafNode *leaf = tree_->find_leaf(current_key_);
      while (leaf != nullptr) {
        auto it =
            std::upper_bound(leaf->keys.begin(), leaf->keys.end(), current_key_, [this](const Key &a, const Key &b) {
              return tree_->compare_(a, b) < 0;
            });
        if (it != leaf->keys.end()) {
          leaf_        = leaf;
          idx_         = static_cast<int>(std::distance(leaf->keys.begin(), it));
          current_key_ = *it;
          valid_       = true;
          return;
        }
        leaf = leaf->next;
      }
      leaf_  = nullptr;
      idx_   = 0;
      valid_ = false;
    }

    void prev()
    {
      std::shared_lock<std::shared_mutex> lock(tree_->rw_lock_);
      if (!valid_ || leaf_ == nullptr) {
        valid_ = false;
        return;
      }
      LeafNode *leaf = tree_->find_leaf(current_key_);
      while (leaf != nullptr) {
        auto it =
            std::lower_bound(leaf->keys.begin(), leaf->keys.end(), current_key_, [this](const Key &a, const Key &b) {
              return tree_->compare_(a, b) < 0;
            });
        if (it != leaf->keys.begin()) {
          --it;
          leaf_        = leaf;
          idx_         = static_cast<int>(std::distance(leaf->keys.begin(), it));
          current_key_ = *it;
          valid_       = true;
          return;
        }
        leaf = leaf->prev;
      }
      leaf_  = nullptr;
      idx_   = 0;
      valid_ = false;
    }

    void seek(const Key &target)
    {
      std::shared_lock<std::shared_mutex> lock(tree_->rw_lock_);
      LeafNode                           *leaf = tree_->find_leaf(target);
      while (leaf != nullptr) {
        auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), target, [this](const Key &a, const Key &b) {
          return tree_->compare_(a, b) < 0;
        });
        if (it != leaf->keys.end()) {
          leaf_        = leaf;
          idx_         = static_cast<int>(std::distance(leaf->keys.begin(), it));
          current_key_ = *it;
          valid_       = true;
          return;
        }
        leaf = leaf->next;
      }
      leaf_  = nullptr;
      idx_   = 0;
      valid_ = false;
    }

    void seek_to_first()
    {
      std::shared_lock<std::shared_mutex> lock(tree_->rw_lock_);
      if (tree_->first_leaf_ != nullptr && !tree_->first_leaf_->keys.empty()) {
        leaf_        = tree_->first_leaf_;
        idx_         = 0;
        current_key_ = leaf_->keys[0];
        valid_       = true;
      } else {
        leaf_  = nullptr;
        idx_   = 0;
        valid_ = false;
      }
    }

    void seek_to_last()
    {
      std::shared_lock<std::shared_mutex> lock(tree_->rw_lock_);
      if (tree_->last_leaf_ != nullptr && !tree_->last_leaf_->keys.empty()) {
        leaf_        = tree_->last_leaf_;
        idx_         = static_cast<int>(leaf_->keys.size()) - 1;
        current_key_ = leaf_->keys[idx_];
        valid_       = true;
      } else {
        leaf_  = nullptr;
        idx_   = 0;
        valid_ = false;
      }
    }

  private:
    const ObMemBPlusTree *tree_;
    LeafNode             *leaf_;
    int                   idx_;
    bool                  valid_;
    Key                   current_key_;
  };

private:
  LeafNode *find_leaf(const Key &key) const
  {
    Node *curr = root_;
    while (curr != nullptr && !curr->is_leaf) {
      InternalNode *internal = static_cast<InternalNode *>(curr);
      size_t        i        = 0;
      while (i < internal->keys.size() && compare_(key, internal->keys[i]) >= 0) {
        i++;
      }
      curr = internal->children[i];
    }
    return static_cast<LeafNode *>(curr);
  }

  struct SplitResult
  {
    bool  did_split = false;
    Key   split_key{};
    Node *right_node = nullptr;
  };

  SplitResult insert_recursive(Node *curr, const Key &key)
  {
    if (curr->is_leaf) {
      LeafNode *leaf = static_cast<LeafNode *>(curr);
      auto      it   = std::lower_bound(
          leaf->keys.begin(), leaf->keys.end(), key, [this](const Key &a, const Key &b) { return compare_(a, b) < 0; });
      if (it != leaf->keys.end() && compare_(*it, key) == 0) {
        return {};
      }
      leaf->keys.insert(it, key);
      if (static_cast<int>(leaf->keys.size()) <= kMaxKeys) {
        return {};
      }

      // Split leaf
      LeafNode *new_leaf = new LeafNode();
      size_t    mid      = leaf->keys.size() / 2;
      new_leaf->keys.assign(leaf->keys.begin() + mid, leaf->keys.end());
      leaf->keys.resize(mid);

      new_leaf->next = leaf->next;
      new_leaf->prev = leaf;
      if (leaf->next != nullptr) {
        leaf->next->prev = new_leaf;
      } else {
        last_leaf_ = new_leaf;
      }
      leaf->next = new_leaf;

      SplitResult res;
      res.did_split  = true;
      res.split_key  = new_leaf->keys.front();
      res.right_node = new_leaf;
      return res;
    }

    InternalNode *internal  = static_cast<InternalNode *>(curr);
    size_t        child_idx = 0;
    while (child_idx < internal->keys.size() && compare_(key, internal->keys[child_idx]) >= 0) {
      child_idx++;
    }

    SplitResult child_res = insert_recursive(internal->children[child_idx], key);
    if (!child_res.did_split) {
      return {};
    }

    internal->keys.insert(internal->keys.begin() + child_idx, child_res.split_key);
    internal->children.insert(internal->children.begin() + child_idx + 1, child_res.right_node);

    if (static_cast<int>(internal->keys.size()) <= kMaxKeys) {
      return {};
    }

    // Split internal node
    InternalNode *new_internal = new InternalNode();
    size_t        mid          = internal->keys.size() / 2;
    Key           up_key       = internal->keys[mid];

    new_internal->keys.assign(internal->keys.begin() + mid + 1, internal->keys.end());
    new_internal->children.assign(internal->children.begin() + mid + 1, internal->children.end());

    internal->keys.resize(mid);
    internal->children.resize(mid + 1);

    SplitResult res;
    res.did_split  = true;
    res.split_key  = up_key;
    res.right_node = new_internal;
    return res;
  }

  void insert_internal(const Key &key)
  {
    SplitResult res = insert_recursive(root_, key);
    if (res.did_split) {
      InternalNode *new_root = new InternalNode();
      new_root->keys.push_back(res.split_key);
      new_root->children.push_back(root_);
      new_root->children.push_back(res.right_node);
      root_ = new_root;
    }
  }

  ObComparator const        compare_;
  Node                     *root_       = nullptr;
  LeafNode                 *first_leaf_ = nullptr;
  LeafNode                 *last_leaf_  = nullptr;
  mutable std::shared_mutex rw_lock_;
};

}  // namespace oceanbase
