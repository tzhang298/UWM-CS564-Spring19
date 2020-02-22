
PRAGMA foreign_keys = ON;

drop trigger if exists trigger9;

create trigger trigger9
	before insert on Bids
	for each row when (NEW.ItemID in (Select i.ItemID From Items i Where i.Currently = i.Buy_Price AND i.Number_of_Bids != 0))
	begin
		SELECT raise(rollback, 'Trigger9_Failed');
	end;
