SELECT
 count(DISTINCT UserID)
FROM
 Users, Items
WHERE
 Users.UserID = Items.SellerID
AND
 Rating > 1000;
