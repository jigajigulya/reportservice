package org.genom.reportservice.utils;

import java.io.Serial;
import java.util.ArrayList;
import java.util.Arrays;

public class TreeNodeList extends ArrayList<TreeNode> {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Optimized set implementation to be used in sorting
     *
     * @param index index of the element to replace
     * @param node node to be stored at the specified position
     * @return the node previously at the specified position
     */
    public TreeNode setSibling(int index, TreeNode node) {
        throw new UnsupportedOperationException();
    }
}
