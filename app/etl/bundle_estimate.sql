with clusters as (
  select distinct cluster_id, sp_pin from episode 
  where  episode_id = 'nd001'
) 
select sp_pin, claim_type, median(allowed_cluster_total )
from cluster_cost_total a, clusters b 
where a.cluster_id = b.cluster_id 
and a.episode_id = 'nd001'
group by sp_pin,claim_type;


#cost per provider per episode, 
select sp_pin, b.claim_type, median(allowed_cluster_total ) over(partition by sp_pin, b.claim_type)
from episode a, cluster_cost_total b
where a.cluster_id = b.cluster_id 
and a.episode_id = 'nd001';
