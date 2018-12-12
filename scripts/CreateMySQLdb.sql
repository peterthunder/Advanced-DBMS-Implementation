CREATE SCHEMA IF NOT EXISTS `project` DEFAULT CHARACTER SET utf8 ;
USE `project` ;

#DROP USER IF EXISTS ‘project_user’@‘localhost’;
CREATE USER IF NOT EXISTS 'project_user'@'localhost' IDENTIFIED BY 'project';
GRANT ALL PRIVILEGES ON * . * TO 'project_user'@'localhost';
FLUSH PRIVILEGES;
