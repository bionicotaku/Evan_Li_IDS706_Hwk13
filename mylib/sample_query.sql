SELECT 
    t1.id, t1.server, t1.seconds_before_next_point, t1.day, t1.opponent, t1.game_score, t1.set, t1.game,
    t2.tournament, t2.surface, t2.seconds_added_per_point, t2.years,
    COUNT(*) as total_rows
FROM 
    serve_times_delta t1
JOIN 
    event_times_delta t2 
    ON t1.id = t2.id
GROUP BY 
    t1.id, t1.server, t1.seconds_before_next_point, t1.day, t1.opponent, t1.game_score, t1.set, t1.game,
    t2.tournament, t2.surface, t2.seconds_added_per_point, t2.years
ORDER BY 
    t1.id, t2.years DESC, t1.seconds_before_next_point;