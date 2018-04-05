---- Valve

-- Latest
select state from Valve where id = (select max(id) from Valve);

-- Update
insert into Valve (state) Values (1);


---- Timer

-- Latest 
select start_time, end_time from Timer where id = (select max(id) from Timer);

-- Update
insert into Timer (start_time, end_time) Values ("06:00", "09:00");


---- WaterFlow

-- Update
insert into WaterFlow (flow) Values (12.4);


---- ValveJob

-- New
insert into ValveJob Values();

-- Update
update ValveJob set status=1 where id=1;

-- Pending
select * from ValveJob where status=0;
