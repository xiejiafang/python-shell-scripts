create procedure p.get_branch()
begin
        declare cursor1 cursor with return for
                select * from administrator.hn_branch;
        open cursor1;
end @   
