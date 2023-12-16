create table nashville_hd (
    UniqueID int,
    ParcelID varchar(255),
    LandUse varchar(255),
    PropertyAddress varchar(255),
    SaleDate text,  
    SalePrice text,
    LegalReference varchar(255),
    SoldAsVacant varchar(3),
    OwnerName varchar(255),
    OwnerAddress varchar(255),
    Acreage decimal(5, 2),
    TaxDistrict varchar(255),
    LandValue int,
    BuildingValue int,
    TotalValue int,
    YearBuilt int,
    Bedrooms int,
    FullBath int,
    HalfBath int
);

copy nashville_hd 
from '/Applications/PostgreSQL 16/Documentation/SQL FIles/Nashville Housing Data for Data Cleaning.csv' 
delimiter ',' csv header;

select * 
from nashville_hd;


-- Changing sale date to appropriate format 
update nashville_hd 
set saledate = to_date(saledate, 'Month DD, YYYY');



-- Changing sale price to appropriate format
-- Cleaning and converting the data
update nashville_hd 
set saleprice = cast(trim(replace(replace(saleprice, '$', ''), ',', '')) as integer);

-- Changing the column type 
alter table nashville_hd
alter column saleprice type integer using saleprice::integer;



-- Populate Property Address data
select *
from nashville_hd
where propertyaddress is null;

-- Confirming that parcelid is unique to address so we can use to populate missing address using self join
select *
from nashville_hd
order by parcelid;

-- Checking to confirm where the above occurred 
select a.parcelid, a.propertyaddress, b.ParcelID, b.PropertyAddress
from nashville_hd a
join nashville_hd b
	on a.parcelid = b.parcelid
	and a.uniqueid <> b.uniqueid 
where a.propertyaddress is null

-- Using the parcelid with address to poplulate the ones without using self join
select a.parcelid, a.propertyaddress, b.ParcelID, b.PropertyAddress, coalesce(a.propertyaddress, b.propertyaddress) as merged_address
from nashville_hd a
join nashville_hd b
	on a.parcelid = b.parcelid
	and a.uniqueid <> b.uniqueid 
where a.propertyaddress is null

-- Making the update
update nashville_hd
set propertyaddress = coalesce(nashville_hd.propertyaddress, b.propertyaddress)
from nashville_hd b
where nashville_hd.propertyaddress is null
  and nashville_hd.parcelid = b.parcelid
  and nashville_hd.uniqueid <> b.uniqueid;



-- Breaking out Address into Individual Columns (Address, City, State)
select propertyaddress 
from nashville_hd;

-- Selecting up till the comma
select 
substring(propertyaddress from 1 for position(',' in propertyaddress) -1) as address
from nashville_hd;

-- Splitting address into 2 
select 
substring(propertyaddress from 1 for position(',' in propertyaddress) -1) as address,
substring (propertyaddress from position(',' in propertyaddress) +1) as city
from nashville_hd;

alter table nashville_hd
add property_split_address varchar(255);

update nashville_hd
set property_split_address = substring(propertyaddress from 1 for position(',' in propertyaddress) -1);

alter table nashville_hd
add property_split_city varchar(255);

update nashville_hd
set property_split_city = substring(propertyaddress from position(',' in propertyaddress) + 1)
where propertyaddress like '%,%';

select *
from nashville_hd;

-- Using split_part to deal with owneraddress
select owneraddress 
from nashville_hd;

select
split_part(replace(owneraddress, ',', '.'), '.', 1),
split_part(replace(owneraddress, ',', '.'), '.', 2),
split_part(replace(owneraddress, ',', '.'), '.', 3)
from nashville_hd;

-- Adding and updating columns
alter table nashville_hd
add owner_split_address varchar(255);

update nashville_hd
set owner_split_address = split_part(replace(owneraddress, ',', '.'), '.', 1);

alter table nashville_hd
add owner_split_city varchar(255);

update nashville_hd
set owner_split_city = split_part(replace(owneraddress, ',', '.'), '.', 2);

alter table nashville_hd
add owner_split_state varchar(255);

update nashville_hd
set owner_split_state = split_part(replace(owneraddress, ',', '.'), '.', 3);

select *
from nashville_hd;



-- Change Y and N to Yes and No in "Sold as Vacant" field
select distinct(soldasvacant), count(soldasvacant)
from nashville_hd
group by soldasvacant
order by 2

-- Checking and updating 
select soldasvacant,
case when soldasvacant = 'Y' then 'Yes'
	 When soldasvacant = 'N' then 'No'
	 else soldasvacant
	 end
from nashville_hd;

update nashville_hd
set soldasvacant = case when soldasvacant = 'Y' then 'Yes'
	   when soldasvacant = 'N' then 'No'
	   else soldasvacant
	   end
	   
	   

-- Remove Duplicates
-- Checking for duplicates
with duplicates_cte as(
Select *,
	row_number() over (
	partition by parcelid,
				 propertyaddress,
				 saleprice,
				 saledate,
				 legalreference
				 order by
					uniqueid
					) row_num
from nashville_hd
--order by ParcelID;
)
Select *
From duplicates_cte
Where row_num > 1
order by propertyaddress;

-- Deleting duplicates
with duplicates_cte as(
Select *,
	row_number() over (
	partition by parcelid,
				 propertyaddress,
				 saleprice,
				 saledate,
				 legalreference
				 order by
					uniqueid
					) row_num
from nashville_hd
)
delete from nashville_hd
where (parcelid, propertyaddress, saleprice, saledate, legalreference) in (
    select parcelid, propertyaddress, saleprice, saledate, legalreference
    from duplicates_cte
    where row_num > 1
);

Select *
from nashville_hd;



-- Delete Unused Columns
select *
from nashville_hd;

alter table nashville_hd
drop column owneraddress, 
drop column propertyaddress;

