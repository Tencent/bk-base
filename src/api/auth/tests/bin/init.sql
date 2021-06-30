-- 依赖auth.sql的单元测试前置sql

INSERT INTO auth_user_role(user_id, role_id, scope_id, created_by, updated_by) VALUES
  ('processor666', 'biz.manager', '591', 'admin', 'admin'),
  ('processor666', 'raw_data.manager', '1', 'admin', 'admin'),
  ('processor666', 'result_table.manager', '591_test_rt', 'admin', 'admin'),
  ('processor666', 'result_table.manager', '591_test_rt2', 'admin', 'admin'),
  ('processor666', 'result_table.manager', '592_test_rt', 'admin', 'admin'),
  ('592_biz_manager', 'biz.manager', '592', 'admin', 'admin'),
  ('593_biz_manager', 'biz.manager', '593', 'admin', 'admin'),
  ('processor666', 'project.manager', '1', 'admin', 'admin'),
  ('666_test_rt_manager', 'result_table.manager', '666_test_rt', 'admin', 'admin'),
  ('processor666', 'result_table.viewer', '666_test_rt', 'admin', 'admin');

INSERT INTO auth_tdw_user(username, tdw_username, tdw_password, created_by, updated_by, description) values
('processor666', 'tdw_user', 'F2NP90mO', 'admin', 'admin', 'xx')
