package com.sdy.domin;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.stream.domain.DimCategoryCompare
 * @Author danyi_shi
 * @Date 2025/5/14 10:57
 * @description:
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimCategoryCompare {
    private Integer id;
    private String categoryName;
    private String searchCategory;
}
