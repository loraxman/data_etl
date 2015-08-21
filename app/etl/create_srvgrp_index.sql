---create index srvgrpX1 on staging.srvgrppabbrev using hash (pin);
--above commented out. not needed since we already resolve this relationship
--in the file preprocessor

--needed to build the provider_loc_search table
create index provsrvlocX1 on staging.provsrvloc(pin,service_location_number);


