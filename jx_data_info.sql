/*
 Navicat Premium Data Transfer

 Source Server         : jiangxin
 Source Server Type    : MySQL
 Source Server Version : 80044 (8.0.44)
 Source Host           : 8.146.210.145:3306
 Source Schema         : jx_data_info

 Target Server Type    : MySQL
 Target Server Version : 80044 (8.0.44)
 File Encoding         : 65001

 Date: 05/03/2026 03:29:53
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for account_daily_tasks
-- ----------------------------
DROP TABLE IF EXISTS `account_daily_tasks`;
CREATE TABLE `account_daily_tasks`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `account_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '账户ID',
  `data_start_date` date NOT NULL COMMENT '数据开始日期',
  `data_end_date` date NOT NULL COMMENT '数据结束日期',
  `store_stats_status` tinyint NULL DEFAULT 0 COMMENT '门店统计-任务状态(0=未执行,1=执行中,2=成功,3=失败)',
  `store_stats_records` int NULL DEFAULT 0 COMMENT '门店统计-记录数',
  `store_stats_error` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '门店统计-错误信息',
  `store_stats_time` datetime NULL DEFAULT NULL COMMENT '门店统计-执行时间',
  `kewen_daily_report_status` tinyint NULL DEFAULT 0 COMMENT '客文日报-任务状态(0=未执行,1=执行中,2=成功,3=失败)',
  `kewen_daily_report_records` int NULL DEFAULT 0 COMMENT '客文日报-记录数',
  `kewen_daily_report_error` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '客文日报-错误信息',
  `kewen_daily_report_time` datetime NULL DEFAULT NULL COMMENT '客文日报-执行时间',
  `promotion_daily_report_status` tinyint NULL DEFAULT 0 COMMENT '推广日报-任务状态(0=未执行,1=执行中,2=成功,3=失败)',
  `promotion_daily_report_records` int NULL DEFAULT 0 COMMENT '推广日报-记录数',
  `promotion_daily_report_error` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '推广日报-错误信息',
  `promotion_daily_report_time` datetime NULL DEFAULT NULL COMMENT '推广日报-执行时间',
  `review_detail_dianping_status` tinyint NULL DEFAULT 0 COMMENT '点评评价明细-任务状态(0=未执行,1=执行中,2=成功,3=失败)',
  `review_detail_dianping_records` int NULL DEFAULT 0 COMMENT '点评评价明细-记录数',
  `review_detail_dianping_error` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '点评评价明细-错误信息',
  `review_detail_dianping_time` datetime NULL DEFAULT NULL COMMENT '点评评价明细-执行时间',
  `review_detail_meituan_status` tinyint NULL DEFAULT 0 COMMENT '美团评价明细-任务状态(0=未执行,1=执行中,2=成功,3=失败)',
  `review_detail_meituan_records` int NULL DEFAULT 0 COMMENT '美团评价明细-记录数',
  `review_detail_meituan_error` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '美团评价明细-错误信息',
  `review_detail_meituan_time` datetime NULL DEFAULT NULL COMMENT '美团评价明细-执行时间',
  `review_summary_dianping_status` tinyint NULL DEFAULT 0 COMMENT '点评评价汇总-任务状态(0=未执行,1=执行中,2=成功,3=失败)',
  `review_summary_dianping_records` int NULL DEFAULT 0 COMMENT '点评评价汇总-记录数',
  `review_summary_dianping_error` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '点评评价汇总-错误信息',
  `review_summary_dianping_time` datetime NULL DEFAULT NULL COMMENT '点评评价汇总-执行时间',
  `review_summary_meituan_status` tinyint NULL DEFAULT 0 COMMENT '美团评价汇总-任务状态(0=未执行,1=执行中,2=成功,3=失败)',
  `review_summary_meituan_records` int NULL DEFAULT 0 COMMENT '美团评价汇总-记录数',
  `review_summary_meituan_error` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '美团评价汇总-错误信息',
  `review_summary_meituan_time` datetime NULL DEFAULT NULL COMMENT '美团评价汇总-执行时间',
  `total_tasks` int NOT NULL DEFAULT 7 COMMENT '总任务数',
  `completed_tasks` int NOT NULL DEFAULT 0 COMMENT '已完成任务数',
  `failed_tasks` int NOT NULL DEFAULT 0 COMMENT '失败任务数',
  `is_all_completed` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否全部完成(0=否,1=是)',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_account_data_range`(`account_id` ASC, `data_start_date` ASC, `data_end_date` ASC) USING BTREE,
  INDEX `idx_data_dates`(`data_start_date` ASC, `data_end_date` ASC) USING BTREE,
  INDEX `idx_is_completed`(`is_all_completed` ASC) USING BTREE,
  INDEX `idx_account`(`account_id` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 13955 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '账号任务执行表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for account_server_mapping
-- ----------------------------
DROP TABLE IF EXISTS `account_server_mapping`;
CREATE TABLE `account_server_mapping`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `account` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT 'platform_accounts的account',
  `server_id` int NOT NULL COMMENT '服务器id',
  `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_account`(`account` ASC) USING BTREE,
  INDEX `idx_server`(`server_id` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 671 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '账号-服务器关联表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cookie_config
-- ----------------------------
DROP TABLE IF EXISTS `cookie_config`;
CREATE TABLE `cookie_config`  (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '唯一标识名',
  `cookies_json` json NOT NULL COMMENT 'cookie数据JSON',
  `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `status` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT 'cookie状态',
  `mtgsig` json NULL COMMENT '签名',
  `compareRegions_json` json NULL COMMENT '商圈数据\r\n',
  `shop_info` json NULL COMMENT '门店详情',
  `templates_id` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '报表模板ID',
  `cookie_refreshed_at` datetime NULL DEFAULT NULL COMMENT '最近一次刷新 / 更新',
  PRIMARY KEY (`id`, `name`) USING BTREE,
  UNIQUE INDEX `uk_name`(`name` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 226718 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = 'Cookie配置表' ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for cookies
-- ----------------------------
DROP TABLE IF EXISTS `cookies`;
CREATE TABLE `cookies`  (
  `id` bigint UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `platform` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '' COMMENT '平台标识，如 facebook / tiktok / shopify',
  `account` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '' COMMENT '账号标识（用户名/邮箱/UID）',
  `cookie_json` json NOT NULL COMMENT 'Cookie JSON 字符串',
  `status` tinyint(1) NOT NULL DEFAULT 1 COMMENT '状态：1=有效 0=失效',
  `remark` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '' COMMENT '备注',
  `expired_at` datetime NULL DEFAULT NULL COMMENT 'Cookie 过期时间，NULL 表示未知',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_platform_account`(`platform` ASC, `account` ASC) USING BTREE,
  INDEX `idx_status`(`status` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 12 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = 'Cookie 存储表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for data_upload_log
-- ----------------------------
DROP TABLE IF EXISTS `data_upload_log`;
CREATE TABLE `data_upload_log`  (
  `id` bigint UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `account_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT 'SaaS账号ID',
  `shop_id` varchar(225) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '门店ID',
  `table_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '数据表名称',
  `data_date_start` date NOT NULL COMMENT '数据开始日期',
  `data_date_end` date NOT NULL COMMENT '数据结束日期',
  `upload_status` tinyint NOT NULL DEFAULT 2 COMMENT '上传状态: 2=成功 3=失败',
  `record_count` int NULL DEFAULT 0 COMMENT '上传记录数',
  `error_message` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT '错误信息',
  `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP COMMENT '上传时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_account_shop`(`account_id` ASC, `shop_id` ASC) USING BTREE,
  INDEX `idx_table_date`(`table_name` ASC, `data_date_start` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 69794 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '数据上传日志表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for douyin_merchant_daily
-- ----------------------------
DROP TABLE IF EXISTS `douyin_merchant_daily`;
CREATE TABLE `douyin_merchant_daily`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键',
  `data_date` date NOT NULL COMMENT '数据日期（这份数据属于哪一天）',
  `merchant_id` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '商家ID',
  `merchant_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '商家名称',
  `industry` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '行业',
  `category` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '类目',
  `cooperation_mode` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '合作模式',
  `follower_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '跟进人',
  `pay_amount` decimal(14, 2) NULL DEFAULT 0.00 COMMENT '支付GMV',
  `confirm_amount` decimal(14, 2) NULL DEFAULT 0.00 COMMENT '核销GMV',
  `refund_amount` decimal(14, 2) NULL DEFAULT 0.00 COMMENT '退款GMV',
  `item_pay_amount` decimal(14, 2) NULL DEFAULT 0.00 COMMENT '视频直接支付GMV',
  `room_pay_amount` decimal(14, 2) NULL DEFAULT 0.00 COMMENT '直播支付GMV',
  `ledger_commission` decimal(14, 2) NULL DEFAULT 0.00 COMMENT '总预估佣金',
  `ledger_smc_commission` decimal(14, 2) NULL DEFAULT 0.00 COMMENT '服务商预估佣金',
  `merchant_manage_score` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '商家经营分',
  `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_date_merchant`(`data_date` ASC, `merchant_id` ASC) USING BTREE,
  INDEX `idx_data_date`(`data_date` ASC) USING BTREE,
  INDEX `idx_merchant_id`(`merchant_id` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '抖音来客商家每日数据' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for kewen_daily_report
-- ----------------------------
DROP TABLE IF EXISTS `kewen_daily_report`;
CREATE TABLE `kewen_daily_report`  (
  `id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
  `report_date` date NOT NULL COMMENT '报表日期',
  `province` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '省份',
  `city` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '城市',
  `shop_id` bigint NOT NULL COMMENT '点评门店ID',
  `shop_name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '门店名称',
  `dianping_star` decimal(3, 1) NULL DEFAULT NULL COMMENT '点评星级',
  `meituan_star` decimal(3, 1) NULL DEFAULT NULL COMMENT '美团星级',
  `operation_score` decimal(5, 2) NULL DEFAULT NULL COMMENT '经营评分(分)',
  `operation_level` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '经营评分牌级',
  `promotion_cost` decimal(12, 2) NULL DEFAULT NULL COMMENT '推广通消耗(元)',
  `merchant_cost` decimal(12, 2) NULL DEFAULT NULL COMMENT '商户通发布额(元)',
  `platform_service_fee` decimal(12, 2) NULL DEFAULT NULL COMMENT '平台技术服务费(元)',
  `commission_gtv` decimal(12, 2) NULL DEFAULT NULL COMMENT '计佣GTV(元)',
  `exposure_users` int NULL DEFAULT NULL COMMENT '曝光人数',
  `exposure_count` int NULL DEFAULT NULL COMMENT '曝光次数',
  `visit_users` int NULL DEFAULT NULL COMMENT '访问人数',
  `visit_count` int NULL DEFAULT NULL COMMENT '访问次数',
  `exposure_visit_rate` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '曝光访问转化率',
  `order_users` int NULL DEFAULT NULL COMMENT '下单人数',
  `lead_users` int NULL DEFAULT NULL COMMENT '留资人数',
  `intent_users` int NULL DEFAULT NULL COMMENT '意向转化人数',
  `intent_rate` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '意向转化率',
  `new_collect_users` int NULL DEFAULT NULL COMMENT '新增收藏人数',
  `total_collect_users` int NULL DEFAULT NULL COMMENT '累计收藏人数',
  `avg_stay_seconds` decimal(8, 2) NULL DEFAULT NULL COMMENT '页面平均停留时长(秒)',
  `promotion_exposure_count` int NULL DEFAULT NULL COMMENT '推广通曝光次数',
  `promotion_click_count` int NULL DEFAULT NULL COMMENT '推广通点击次数',
  `verify_sale_amount` decimal(12, 2) NULL DEFAULT NULL COMMENT '核销售价金额(元)',
  `verify_after_discount` decimal(12, 2) NULL DEFAULT NULL COMMENT '商家优惠后核销额(元)',
  `verify_coupon_count` int NULL DEFAULT NULL COMMENT '核销券数(张)',
  `verify_order_count` int NULL DEFAULT NULL COMMENT '核销订单量(单)',
  `verify_person_count` int NULL DEFAULT NULL COMMENT '核销人次',
  `verify_new_customer` int NULL DEFAULT NULL COMMENT '核销新客人数',
  `order_coupon_count` int NULL DEFAULT NULL COMMENT '下单券数(张)',
  `order_sale_amount` decimal(12, 2) NULL DEFAULT NULL COMMENT '下单售价金额(元)',
  `consult_users` int NULL DEFAULT NULL COMMENT '在线咨询人数',
  `consult_lead_count` int NULL DEFAULT NULL COMMENT '在线咨询留资数',
  `consult_lead_rate` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '咨询留资转化率',
  `avg_response_seconds` decimal(8, 2) NULL DEFAULT NULL COMMENT '平均响应时长(秒)',
  `reply_rate_30s` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '30秒内回复率',
  `reply_rate_5min` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '5分钟内回复率',
  `refund_amount` decimal(12, 2) NULL DEFAULT NULL COMMENT '退款售价金额(元)',
  `refund_order_count` int NULL DEFAULT NULL COMMENT '退款订单量(单)',
  `refund_users` int NULL DEFAULT NULL COMMENT '退款人数',
  `complaint_count` int NULL DEFAULT NULL COMMENT '消费纠纷投诉量(单)',
  `compensation_order_count` int NULL DEFAULT NULL COMMENT '投诉赔付订单量(单)',
  `new_review_count` int NULL DEFAULT NULL COMMENT '新增评价数(条)',
  `new_good_review_count` int NULL DEFAULT NULL COMMENT '新增好评数(条)',
  `new_medium_review_count` int NULL DEFAULT NULL COMMENT '新增中评数(条)',
  `new_bad_review_count` int NULL DEFAULT NULL COMMENT '新增差评数(条)',
  `bad_review_reply_rate` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '差评回复率',
  `total_review_count` int NULL DEFAULT NULL COMMENT '累计评价数(条)',
  `total_bad_review_count` int NULL DEFAULT NULL COMMENT '累计差评数(条)',
  `coupon_code_type` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '优惠码类型(全部码/门店码/商品码/职人码/品牌码)',
  `coupon_pay_order_count` int NULL DEFAULT NULL COMMENT '优惠码支付订单数(个)',
  `coupon_pay_amount` decimal(12, 2) NULL DEFAULT NULL COMMENT '优惠码支付金额(元)',
  `coupon_verify_amount` decimal(12, 2) NULL DEFAULT NULL COMMENT '优惠码核销金额(元)',
  `coupon_scan_users` int NULL DEFAULT NULL COMMENT '优惠码扫码人数(人)',
  `coupon_scan_collect_count` int NULL DEFAULT NULL COMMENT '优惠码扫码收藏数(个)',
  `coupon_scan_review_count` int NULL DEFAULT NULL COMMENT '优惠码扫码评价数(个)',
  `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_date_shop`(`report_date` ASC, `shop_id` ASC) USING BTREE,
  INDEX `idx_shop_id`(`shop_id` ASC) USING BTREE,
  INDEX `idx_report_date`(`report_date` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 25586 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '可问综合日报表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for platform_accounts
-- ----------------------------
DROP TABLE IF EXISTS `platform_accounts`;
CREATE TABLE `platform_accounts`  (
  `account` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '平台账号',
  `operator_id` int UNSIGNED NULL DEFAULT NULL COMMENT '所属运营ID',
  `cookie` json NULL COMMENT '登录Cookie',
  `auth_status` enum('valid','invalid') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT 'valid' COMMENT '登录授权: valid-有效 invalid-无效',
  `brands_json` json NULL COMMENT '广告单数据JSON',
  `compareRegions_json` json NULL COMMENT '商圈数据',
  `stores_json` json NULL COMMENT '门店数据JSON',
  `mtgsig` json NULL COMMENT '美团签名',
  `templates_id` int NULL DEFAULT NULL COMMENT '报表ID',
  `sales_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '销售',
  `city_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '城市',
  `dele_id` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '删除门店ID',
  `status` tinyint(1) NULL DEFAULT 1 COMMENT '状态: 1-启用 0-禁用',
  `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`account`) USING BTREE,
  UNIQUE INDEX `uk_account`(`account` ASC) USING BTREE,
  INDEX `idx_operator_id`(`operator_id` ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '平台账号表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for promotion_daily_report
-- ----------------------------
DROP TABLE IF EXISTS `promotion_daily_report`;
CREATE TABLE `promotion_daily_report`  (
  `id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
  `report_date` date NOT NULL COMMENT '报表日期',
  `shop_id` bigint NOT NULL COMMENT '门店ID',
  `shop_name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '推广门店名称',
  `city_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '门店所在城市',
  `cost` decimal(12, 2) NULL DEFAULT 0.00 COMMENT '花费(元)',
  `exposure_count` int NULL DEFAULT 0 COMMENT '曝光(次)',
  `click_count` int NULL DEFAULT 0 COMMENT '点击(次)',
  `click_avg_price` decimal(8, 2) NULL DEFAULT 0.00 COMMENT '点击均价(元)',
  `shop_view_count` int NULL DEFAULT NULL COMMENT '商户浏览量(次)',
  `coupon_order_count` int NULL DEFAULT NULL COMMENT '优惠预订订单量(个)',
  `groupbuy_order_count` int NULL DEFAULT NULL COMMENT '团购订单量(个)',
  `order_count` int NULL DEFAULT NULL COMMENT '订单量(个)',
  `view_pic_count` int NULL DEFAULT NULL COMMENT '查看图片(次)',
  `view_comment_count` int NULL DEFAULT NULL COMMENT '查看评论(次)',
  `view_address_count` int NULL DEFAULT NULL COMMENT '查看地址(次)',
  `view_phone_count` int NULL DEFAULT NULL COMMENT '查看电话(次)',
  `view_groupbuy_count` int NULL DEFAULT NULL COMMENT '查看团购(次)',
  `collect_count` int NULL DEFAULT NULL COMMENT '收藏(次)',
  `share_count` int NULL DEFAULT NULL COMMENT '分享(次)',
  `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_date_shop`(`report_date` ASC, `shop_id` ASC) USING BTREE,
  INDEX `idx_shop_id`(`shop_id` ASC) USING BTREE,
  INDEX `idx_report_date`(`report_date` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 17368 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '推广日报表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for review_detail_dianping
-- ----------------------------
DROP TABLE IF EXISTS `review_detail_dianping`;
CREATE TABLE `review_detail_dianping`  (
  `id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
  `review_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '评价ID',
  `shop_id` bigint NOT NULL COMMENT '门店ID',
  `shop_name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '门店名称',
  `city_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '城市',
  `city_id` int NULL DEFAULT NULL COMMENT '城市ID',
  `user_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '用户ID',
  `user_nickname` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '用户昵称',
  `user_face` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '用户头像URL',
  `user_power` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '用户等级',
  `vip_level` int NULL DEFAULT NULL COMMENT 'VIP等级',
  `add_time` datetime NULL DEFAULT NULL COMMENT '评价时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  `edit_time` datetime NULL DEFAULT NULL COMMENT '编辑时间',
  `star` int NULL DEFAULT NULL COMMENT '星级(原始值,50=5星)',
  `star_display` decimal(2, 1) NULL DEFAULT NULL COMMENT '星级(1-5)',
  `accurate_star` int NULL DEFAULT NULL COMMENT '精确星级',
  `content` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT '评价内容',
  `score_technician` decimal(2, 1) NULL DEFAULT NULL COMMENT '技师评分',
  `score_service` decimal(2, 1) NULL DEFAULT NULL COMMENT '服务评分',
  `score_environment` decimal(2, 1) NULL DEFAULT NULL COMMENT '环境评分',
  `score_map` json NULL COMMENT '评分详情JSON',
  `pic_count` int NULL DEFAULT 0 COMMENT '图片数量',
  `video_count` int NULL DEFAULT 0 COMMENT '视频数量',
  `pic_info` json NULL COMMENT '图片信息[{pic_id,pic_url,origin_pic_url}]',
  `video_info` json NULL COMMENT '视频信息',
  `shop_reply` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT '商家回复内容(最新一条)',
  `shop_reply_time` datetime NULL DEFAULT NULL COMMENT '商家回复时间',
  `is_reply_with_photo` tinyint NULL DEFAULT 0 COMMENT '回复是否带图',
  `reply_list` json NULL COMMENT '所有回复列表',
  `order_id` bigint NULL DEFAULT NULL COMMENT '订单ID',
  `deal_group_id` bigint NULL DEFAULT NULL COMMENT '团购ID',
  `refer_type` int NULL DEFAULT NULL COMMENT '来源类型',
  `avg_price` int NULL DEFAULT NULL COMMENT '人均价格',
  `serial_numbers` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '券号',
  `total_cost` decimal(12, 2) NULL DEFAULT NULL COMMENT '总价',
  `consume_date` date NULL DEFAULT NULL COMMENT '消费日期',
  `status` int NULL DEFAULT NULL COMMENT '评价状态',
  `quality_score` int NULL DEFAULT NULL COMMENT '质量分',
  `case_status` int NULL DEFAULT NULL COMMENT '投诉状态',
  `case_status_desc` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '投诉状态描述',
  `report_status` int NULL DEFAULT NULL COMMENT '举报状态',
  `report_status_desc` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '举报状态描述',
  `case_id` bigint NULL DEFAULT NULL COMMENT '投诉ID',
  `show_deal` tinyint NULL DEFAULT NULL COMMENT '是否显示交易',
  `raw_data` json NULL COMMENT '原始JSON数据',
  `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ai_gen` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT 'AI生成内容',
  `manual_confirm` int NOT NULL DEFAULT 0 COMMENT '人工确认 0-未确认 1-已确认',
  `task_reply` int NOT NULL DEFAULT 0 COMMENT '任务回复 0-未回复 1-已回复 2-回复失败',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_review_id`(`review_id` ASC) USING BTREE,
  INDEX `idx_shop_id`(`shop_id` ASC) USING BTREE,
  INDEX `idx_add_time`(`add_time` ASC) USING BTREE,
  INDEX `idx_star`(`star` ASC) USING BTREE,
  INDEX `idx_user_id`(`user_id` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 11815 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '点评评价详情表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for review_detail_meituan
-- ----------------------------
DROP TABLE IF EXISTS `review_detail_meituan`;
CREATE TABLE `review_detail_meituan`  (
  `id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
  `review_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '评价ID',
  `feedback_id` bigint NULL DEFAULT NULL COMMENT '反馈ID',
  `shop_id` bigint NOT NULL COMMENT '门店ID',
  `shop_name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '门店名称',
  `city_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '城市',
  `city_id` int NULL DEFAULT NULL COMMENT '城市ID',
  `user_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '用户ID',
  `user_nickname` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '用户昵称',
  `user_face` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '用户头像URL',
  `user_power` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '用户等级',
  `anonymous` tinyint NULL DEFAULT 0 COMMENT '是否匿名 0=否 1=是',
  `add_time` datetime NULL DEFAULT NULL COMMENT '评价时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  `edit_time` datetime NULL DEFAULT NULL COMMENT '编辑时间',
  `star` int NULL DEFAULT NULL COMMENT '星级(原始值,50=5星)',
  `star_display` decimal(2, 1) NULL DEFAULT NULL COMMENT '星级(1-5)',
  `accurate_star` int NULL DEFAULT NULL COMMENT '精确星级',
  `content` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT '评价内容',
  `pic_count` int NULL DEFAULT 0 COMMENT '图片数量',
  `video_count` int NULL DEFAULT 0 COMMENT '视频数量',
  `pic_info` json NULL COMMENT '图片信息[{pic_id,pic_url,origin_pic_url}]',
  `video_info` json NULL COMMENT '视频信息',
  `shop_reply` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT '商家回复内容(最新一条)',
  `shop_reply_time` datetime NULL DEFAULT NULL COMMENT '商家回复时间',
  `reply_list` json NULL COMMENT '所有回复列表',
  `order_id` bigint NULL DEFAULT NULL COMMENT '订单ID',
  `refer_type` int NULL DEFAULT NULL COMMENT '来源类型',
  `business_type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '业务类型',
  `coupon_code` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '券号',
  `product_name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '项目名称',
  `order_time` datetime NULL DEFAULT NULL COMMENT '下单时间',
  `consume_time` datetime NULL DEFAULT NULL COMMENT '消费时间',
  `quantity` int NULL DEFAULT NULL COMMENT '数量',
  `price` decimal(12, 2) NULL DEFAULT NULL COMMENT '总价',
  `case_status` int NULL DEFAULT NULL COMMENT '投诉状态',
  `report_status` int NULL DEFAULT NULL COMMENT '举报状态',
  `case_id` bigint NULL DEFAULT NULL COMMENT '投诉ID',
  `show_deal` tinyint NULL DEFAULT NULL COMMENT '是否显示交易',
  `raw_data` json NULL COMMENT '原始JSON数据',
  `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ai_gen` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT 'AI生成内容',
  `manual_confirm` int NOT NULL DEFAULT 0 COMMENT '人工确认 0-未确认 1-已确认',
  `task_reply` int NOT NULL DEFAULT 0 COMMENT '任务回复 0-未回复 1-已回复 2-回复失败',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_review_id`(`review_id` ASC) USING BTREE,
  INDEX `idx_shop_id`(`shop_id` ASC) USING BTREE,
  INDEX `idx_add_time`(`add_time` ASC) USING BTREE,
  INDEX `idx_star`(`star` ASC) USING BTREE,
  INDEX `idx_user_id`(`user_id` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 50673 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '美团评价详情表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for review_summary_dianping
-- ----------------------------
DROP TABLE IF EXISTS `review_summary_dianping`;
CREATE TABLE `review_summary_dianping`  (
  `id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
  `review_time` datetime NOT NULL COMMENT '评价时间',
  `city` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '城市',
  `shop_name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '评价门店',
  `dianping_shop_id` bigint NOT NULL COMMENT '点评门店ID',
  `meituan_shop_id` bigint NULL DEFAULT NULL COMMENT '美团门店ID',
  `user_nickname` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '用户昵称',
  `star` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '星级',
  `score_detail` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '评分详情(技师:x,服务:x,环境:x)',
  `content` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT '评价内容',
  `content_length` int NULL DEFAULT 0 COMMENT '评价正文字数',
  `pic_count` int NULL DEFAULT 0 COMMENT '图片数',
  `video_count` int NULL DEFAULT 0 COMMENT '视频数',
  `is_replied` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '商家是否已回复',
  `first_reply_time` datetime NULL DEFAULT NULL COMMENT '商家首次回复时间',
  `is_after_consume` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '是否消费后评价',
  `consume_time` date NULL DEFAULT NULL COMMENT '消费时间',
  `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`, `review_time`, `dianping_shop_id`) USING BTREE,
  UNIQUE INDEX `uk_review_time_shop`(`review_time` ASC, `dianping_shop_id` ASC) USING BTREE,
  INDEX `idx_review_time`(`review_time` ASC) USING BTREE,
  INDEX `idx_shop_id`(`dianping_shop_id` ASC) USING BTREE,
  INDEX `idx_star`(`star` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 721 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '点评评价汇总表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for review_summary_meituan
-- ----------------------------
DROP TABLE IF EXISTS `review_summary_meituan`;
CREATE TABLE `review_summary_meituan`  (
  `id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
  `review_time` datetime NOT NULL COMMENT '评价时间',
  `city` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '城市',
  `shop_name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '评价门店',
  `dianping_shop_id` bigint NULL DEFAULT NULL COMMENT '点评门店ID',
  `meituan_shop_id` bigint NOT NULL COMMENT '美团门店ID',
  `user_nickname` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '用户昵称',
  `star` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '星级',
  `content` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT '评价内容',
  `content_length` int NULL DEFAULT 0 COMMENT '评价正文字数',
  `pic_count` int NULL DEFAULT 0 COMMENT '图片数',
  `video_count` int NULL DEFAULT 0 COMMENT '视频数',
  `is_replied` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '商家是否已回复',
  `first_reply_time` datetime NULL DEFAULT NULL COMMENT '商家首次回复时间',
  `is_after_consume` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '是否消费后评价',
  `consume_time` date NULL DEFAULT NULL COMMENT '消费时间',
  `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`, `review_time`, `meituan_shop_id`) USING BTREE,
  UNIQUE INDEX `uk_review_time_shop`(`review_time` ASC, `meituan_shop_id` ASC) USING BTREE,
  UNIQUE INDEX `uk_review_meituan`(`review_time` ASC, `meituan_shop_id` ASC) USING BTREE,
  INDEX `idx_review_time`(`review_time` ASC) USING BTREE,
  INDEX `idx_shop_id`(`meituan_shop_id` ASC) USING BTREE,
  INDEX `idx_star`(`star` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2068 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '美团评价汇总表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for saas_users
-- ----------------------------
DROP TABLE IF EXISTS `saas_users`;
CREATE TABLE `saas_users`  (
  `id` int UNSIGNED NOT NULL AUTO_INCREMENT,
  `username` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '登录用户名',
  `password` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '登录密码',
  `name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '运营姓名',
  `role` enum('super_admin','operation_manager','operation_specialist','intern_manager','intern_specialist') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '角色',
  `manager_id` int UNSIGNED NULL DEFAULT NULL COMMENT '上级领导ID',
  `status` tinyint(1) NULL DEFAULT 1 COMMENT '状态: 1-启用 0-禁用',
  `last_login_at` datetime NULL DEFAULT NULL COMMENT '最后登录时间',
  `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_username`(`username` ASC) USING BTREE,
  INDEX `fk_manager`(`manager_id` ASC) USING BTREE,
  CONSTRAINT `fk_manager` FOREIGN KEY (`manager_id`) REFERENCES `saas_users` (`id`) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE = InnoDB AUTO_INCREMENT = 75 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = 'SaaS用户表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for servers
-- ----------------------------
DROP TABLE IF EXISTS `servers`;
CREATE TABLE `servers`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `server_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '服务器名称',
  `server_host` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '服务器地址',
  `max_accounts` int NULL DEFAULT 100 COMMENT '最大账号数',
  `current_count` int NULL DEFAULT 0 COMMENT '当前账号数',
  `status` enum('active','inactive','full') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT 'active' COMMENT '状态',
  `remark` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '备注',
  `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 7 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '服务器表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for store_stats
-- ----------------------------
DROP TABLE IF EXISTS `store_stats`;
CREATE TABLE `store_stats`  (
  `id` bigint UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `store_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '门店名称',
  `store_id` bigint NOT NULL,
  `checkin_count` int UNSIGNED NOT NULL DEFAULT 0 COMMENT '打卡数',
  `order_user_rank` int UNSIGNED NULL DEFAULT NULL COMMENT '下单人数商圈排名',
  `verify_amount_rank` int UNSIGNED NULL DEFAULT NULL COMMENT '核销金额商圈排名',
  `ad_order_count` int UNSIGNED NOT NULL DEFAULT 0 COMMENT '广告单',
  `is_force_offline` int NOT NULL DEFAULT 0 COMMENT '强制下线数量',
  `ad_balance` float NULL DEFAULT NULL COMMENT '推广通余额',
  `date` date NOT NULL COMMENT '数据日期',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`, `store_id`, `date`) USING BTREE,
  UNIQUE INDEX `uk_store_date`(`store_id` ASC, `date` ASC) USING BTREE,
  INDEX `idx_store_id`(`store_id` ASC) USING BTREE,
  INDEX `idx_created_at`(`created_at` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 16911 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '门店统计数据表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for store_strategy_config
-- ----------------------------
DROP TABLE IF EXISTS `store_strategy_config`;
CREATE TABLE `store_strategy_config`  (
  `store_id` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '门店ID',
  `review_ending` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL COMMENT '评价结尾',
  `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`store_id`) USING BTREE,
  UNIQUE INDEX `uk_store_id`(`store_id` ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '门店策略配置表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for task_schedule
-- ----------------------------
DROP TABLE IF EXISTS `task_schedule`;
CREATE TABLE `task_schedule`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `account_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '账户ID',
  `task_date` date NOT NULL COMMENT '任务日期(当天)',
  `data_start_date` date NOT NULL COMMENT '数据开始日期(前天)',
  `data_end_date` date NOT NULL COMMENT '数据结束日期(昨天)',
  `task_type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT 'all' COMMENT '任务类型: all/store_stats/kewen_daily_report/promotion_daily_report/review_detail_dianping/review_detail_meituan/review_summary_dianping/review_summary_meituan',
  `status` tinyint NOT NULL DEFAULT 0 COMMENT '状态: 0=待执行, 1=执行中, 2=已完成, 3=失败，4=无权限',
  `locked_by` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '锁定的worker标识',
  `locked_at` datetime NULL DEFAULT NULL COMMENT '锁定时间',
  `retry_count` int NOT NULL DEFAULT 0 COMMENT '重试次数',
  `error_message` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '错误信息',
  `server` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '服务器编号',
  `started_at` datetime NULL DEFAULT NULL COMMENT '开始执行时间',
  `completed_at` datetime NULL DEFAULT NULL COMMENT '完成时间',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_account_date_type`(`account_id` ASC, `task_date` ASC, `task_type` ASC) USING BTREE,
  INDEX `idx_task_date`(`task_date` ASC) USING BTREE,
  INDEX `idx_status`(`status` ASC) USING BTREE,
  INDEX `idx_locked`(`locked_by` ASC, `locked_at` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 93799 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '任务调度表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Procedure structure for batch_assign_servers
-- ----------------------------
DROP PROCEDURE IF EXISTS `batch_assign_servers`;
delimiter ;;
CREATE PROCEDURE `batch_assign_servers`()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE acc VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    DECLARE available_server_id INT;
    
    DECLARE cur CURSOR FOR 
        SELECT p.account 
        FROM platform_accounts p
        LEFT JOIN account_server_mapping m 
            ON p.account COLLATE utf8mb4_unicode_ci = m.account COLLATE utf8mb4_unicode_ci
        WHERE m.account IS NULL;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    OPEN cur;
    
    read_loop: LOOP
        FETCH cur INTO acc;
        IF done THEN
            LEAVE read_loop;
        END IF;
        
        SELECT id INTO available_server_id
        FROM servers
        WHERE status = 'active' AND current_count < max_accounts
        ORDER BY current_count ASC
        LIMIT 1;
        
        IF available_server_id IS NOT NULL THEN
            INSERT INTO account_server_mapping (account, server_id)
            VALUES (acc, available_server_id);
            
            UPDATE servers 
            SET current_count = current_count + 1,
                status = IF(current_count + 1 >= max_accounts, 'full', 'active')
            WHERE id = available_server_id;
        END IF;
    END LOOP;
    
    CLOSE cur;
END
;;
delimiter ;

-- ----------------------------
-- Triggers structure for table platform_accounts
-- ----------------------------
DROP TRIGGER IF EXISTS `auto_release_server`;
delimiter ;;
CREATE TRIGGER `auto_release_server` BEFORE DELETE ON `platform_accounts` FOR EACH ROW BEGIN
    DECLARE old_server_id INT;
    
    SELECT server_id INTO old_server_id
    FROM account_server_mapping
    WHERE account = OLD.account;
    
    IF old_server_id IS NOT NULL THEN
        DELETE FROM account_server_mapping WHERE account = OLD.account;
        
        UPDATE servers 
        SET current_count = GREATEST(current_count - 1, 0),
            status = 'active'
        WHERE id = old_server_id;
    END IF;
END
;;
delimiter ;

-- ----------------------------
-- Triggers structure for table platform_accounts
-- ----------------------------
DROP TRIGGER IF EXISTS `auto_assign_server`;
delimiter ;;
CREATE TRIGGER `auto_assign_server` AFTER INSERT ON `platform_accounts` FOR EACH ROW BEGIN
    DECLARE available_server_id INT;
    
    SELECT id INTO available_server_id
    FROM servers
    WHERE status = 'active' AND current_count < max_accounts
    ORDER BY current_count ASC
    LIMIT 1;
    
    IF available_server_id IS NOT NULL THEN
        INSERT INTO account_server_mapping (account, server_id)
        VALUES (NEW.account, available_server_id);
        
        UPDATE servers 
        SET current_count = current_count + 1,
            status = IF(current_count + 1 >= max_accounts, 'full', 'active')
        WHERE id = available_server_id;
    END IF;
END
;;
delimiter ;

SET FOREIGN_KEY_CHECKS = 1;
