truncate table staging.provlddelta;
insert into staging.provlddelta ( pin ,
    service_location_number,
      change_type ,
        change_date ,
          consumed_date )

select pin, service_location_number,'ADD',current_time, null
from staging.provsrvloc;

commit;

