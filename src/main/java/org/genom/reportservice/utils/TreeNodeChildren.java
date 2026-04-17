
package org.genom.reportservice.utils;

import java.io.Serial;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class TreeNodeChildren extends TreeNodeList {

    @Serial
    private static final long serialVersionUID = 1L;

    private TreeNode parent;

    public TreeNodeChildren(TreeNode parent) {
        this.parent = parent;
    }

    private void eraseParent(TreeNode node) {
        TreeNode parentNode = node.getParent();
        if (parentNode != null) {
            parentNode.getChildren().remove(node);
            node.setParent(null);
        }
    }


    public boolean add(TreeNode node) {
        if (node == null) {
            throw new NullPointerException();
        }

        eraseParent(node);
        boolean result = super.add(node);
        node.setParent(parent);
        updateRowKeys(parent.getChildCount() - 1, parent);
        return result;
    }


    public void add(int index, TreeNode node) {
        if (node == null) {
            throw new NullPointerException();
        }

        if (index < 0 || index > size()) {
            throw new IndexOutOfBoundsException();
        }

        eraseParent(node);
        super.add(index, node);
        node.setParent(parent);
        updateRowKeys(index, parent);
    }


    public boolean addAll(Collection<? extends TreeNode> collection) {
        Iterator<TreeNode> elements = (new ArrayList<TreeNode>(collection)).iterator();
        int size = this.size();
        boolean changed = false;
        while (elements.hasNext()) {
            TreeNode node = elements.next();
            if (node == null) {
                throw new NullPointerException();
            }

            eraseParent(node);
            super.add(node);
            node.setParent(parent);
            changed = true;
        }

        if (changed) {
            updateRowKeys(size, parent);
        }

        return changed;
    }


    public boolean addAll(int index, Collection<? extends TreeNode> collection) {
        Iterator<TreeNode> elements = (new ArrayList<TreeNode>(collection)).iterator();
        boolean changed = false;
        while (elements.hasNext()) {
            TreeNode node = elements.next();
            if (node == null) {
                throw new NullPointerException();
            }

            eraseParent(node);
            super.add(index++, node);
            node.setParent(parent);
            changed = true;
        }

        if (changed) {
            updateRowKeys(index, parent);
        }

        return changed;
    }


    public TreeNode set(int index, TreeNode node) {
        if (node == null) {
            throw new NullPointerException();
        }
        if (index < 0 || index >= size()) {
            throw new IndexOutOfBoundsException();
        }
        if (!parent.equals(node.getParent())) {
            eraseParent(node);
        }

        TreeNode previous = get(index);
        super.set(index, node);
        previous.setParent(null);
        node.setParent(parent);
        updateRowKeys(parent, node, index);
        return previous;
    }

    /**
     * Optimized set implementation to be used in sorting
     *
     * @param index index of the element to replace
     * @param node node to be stored at the specified position
     * @return the node previously at the specified position
     */

    public TreeNode setSibling(int index, TreeNode node) {
        if (node == null) {
            throw new NullPointerException();
        }
        if (index < 0 || index >= size()) {
            throw new IndexOutOfBoundsException();
        }
        if (!parent.equals(node.getParent())) {
            eraseParent(node);
        }
        TreeNode previous = get(index);
        super.set(index, node);
        node.setParent(parent);
        updateRowKeys(parent, node, index);
        return previous;
    }


    public TreeNode remove(int index) {
        TreeNode node = get(index);
        node.setParent(null);
        super.remove(index);
        updateRowKeys(index, parent);
        return node;
    }


    public boolean remove(Object object) {
        TreeNode node = (TreeNode) object;
        if (node == null) {
            throw new NullPointerException();
        }

        if (super.indexOf(node) != -1) {
            node.clearParent();
        }

        int index = super.indexOf(node);
        if (super.remove(node)) {
            updateRowKeys(index, parent);
            return true;
        }
        else {
            return false;
        }
    }

    private void updateRowKeys(TreeNode node) {
        int childCount = node.getChildCount();
        if (childCount > 0) {
            for (int i = 0; i < childCount; i++) {
                TreeNode childNode = node.getChildren().get(i);

                updateRowKeys(node, childNode, i);
            }
        }
    }

    private void updateRowKeys(int index, TreeNode node) {
        int childCount = node.getChildCount();
        if (childCount > 0) {
            for (int i = index; i < childCount; i++) {
                TreeNode childNode = node.getChildren().get(i);
                updateRowKeys(node, childNode, i);
            }
        }
    }

    private void updateRowKeys(TreeNode node, TreeNode childNode, int i) {
        String childRowKey = node.getParent() == null ? String.valueOf(i) : node.getRowKey() + "_" + i;
        childNode.setRowKey(childRowKey);
        this.updateRowKeys(childNode);
    }

}
