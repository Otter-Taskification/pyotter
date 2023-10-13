-- Get the location refs and event positions of a chunk
select location_ref
    ,event_pos
from chunk_contents
where chunk_key = ?
order by rowid
;
