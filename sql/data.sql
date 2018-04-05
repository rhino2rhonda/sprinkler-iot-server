use sprinkler;

-- User
insert into user (id, username, password) Values (1, 'yolo', '123');
insert into user (id, username, password) Values (2, 'test', 'test');

-- Product
insert into product_type (id, product_name) Values(1, 'sprinkler');
insert into product (id, user_id, product_type_id, product_key) Values(1, 1, 1, 'a2F#Da$@3fa34s&*f');
insert into product (id, user_id, product_type_id, product_key) Values(2, 2, 1, '$(2#2Da$131s&*f4!x');

-- Components
insert into component_type (id, component_name, product_type_id) Values(1, 'valve', 1);
insert into component_type (id, component_name, product_type_id) Values(2, 'flow-sensor', 1);
insert into component (id, component_type_id, product_id) Values(1, 1, 1);
insert into component (id, component_type_id, product_id) Values(2, 2, 1);
insert into component (id, component_type_id, product_id) Values(3, 1, 2);
insert into component (id, component_type_id, product_id) Values(4, 2, 2);

-- Valve Controllers
insert into valve_controller_type (id, controller_name) Values(1, 'remote-switch');
insert into valve_controller_type (id, controller_name) Values(2, 'timer');