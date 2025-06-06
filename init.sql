CREATE DATABASE IF NOT EXISTS userdb;

USE userdb;

CREATE TABLE IF NOT EXISTS logins (
    S_no INT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(255),
    login_location VARCHAR(255),
    login_date DATETIME,
    topic VARCHAR(255)
);
