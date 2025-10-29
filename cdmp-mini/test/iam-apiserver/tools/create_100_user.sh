-- 创建存储过程批量插入100万条用户数据
DELIMITER //

CREATE PROCEDURE GenerateMillionUsers()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE batch_size INT DEFAULT 1000;
    DECLARE total_records INT DEFAULT 1000000;
    DECLARE start_time TIMESTAMP;
    
    SET start_time = NOW();
    
    WHILE i <= total_records DO
        -- 每1000条提交一次
        IF i % batch_size = 1 THEN
            START TRANSACTION;
        END IF;
        
        INSERT INTO user (
            instanceID, 
            name, 
            status, 
            nickname, 
            password, 
            email, 
            phone, 
            isAdmin, 
            loginedAt,
            extendShadow
        ) VALUES (
            CONCAT('inst_', LPAD(FLOOR((i-1)/200000) + 1, 3, '0')),  -- 5个instance
            CONCAT('user_', LPAD(i, 7, '0')),                        -- 唯一用户名
            CASE 
                WHEN i % 100 = 0 THEN 0                             -- 1% status=0
                WHEN i % 50 = 0 THEN 2                              -- 2% status=2
                ELSE 1                                              -- 97% status=1
            END,
            CONCAT('nick', i % 10000),                              -- 昵称会有重复
            CONCAT('$2a$10$', SUBSTRING(SHA2(CONCAT('password', i), 256), 1, 53)),
            CONCAT('user', i, '@', 
                   CASE (i % 5) 
                       WHEN 0 THEN 'gmail.com' 
                       WHEN 1 THEN 'example.com' 
                       WHEN 2 THEN 'company.org'
                       WHEN 3 THEN 'test.com'
                       ELSE 'email.net'
                   END),
            CONCAT('1', 
                   CASE (i % 6)
                       WHEN 0 THEN '38'
                       WHEN 1 THEN '39' 
                       WHEN 2 THEN '86'
                       WHEN 3 THEN '76'
                       WHEN 4 THEN '85'
                       ELSE '87'
                   END, 
                   LPAD(100000000 + i, 9, '0')),
            IF(i % 5000 = 0, 1, 0),                                -- 0.02% 是管理员
            IF(i % 8 = 0, 
               DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 730) DAY),   -- 12.5% 用户有登录时间
               NULL),
            IF(i % 20 = 0, '{"preferences": {"theme": "dark"}}', NULL)  -- 5%用户有扩展字段
        );
        
        -- 提交批次
        IF i % batch_size = 0 OR i = total_records THEN
            COMMIT;
        END IF;
        
        SET i = i + 1;
        
        -- 进度提示（每5万条）
        IF i % 50000 = 0 THEN
            SELECT CONCAT('已插入: ', i, ' 条记录, 耗时: ', 
                         TIMESTAMPDIFF(SECOND, start_time, NOW()), '秒') as progress;
        END IF;
    END WHILE;
    
    SELECT CONCAT('数据插入完成，总计100万条记录，总耗时: ', 
                 TIMESTAMPDIFF(SECOND, start_time, NOW()), '秒') as completion_message;
END//

DELIMITER ;

-- 执行存储过程
CALL GenerateMillionUsers();

-- 清理存储过程
DROP PROCEDURE GenerateMillionUsers;