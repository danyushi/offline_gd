package com.sdy.domin;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.sdy.domin.Orderinfo
 * @Author danyu-shi
 * @Date 2025/5/14 16:29
 * @description:
 */

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Orderinfo {
    private String id;
    private String user_id;
    private String total_amount;

}
