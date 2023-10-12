--truncate table field_demos.uc_lineage.t_reporting_user_history

Create or replace table field_demos.uc_lineage.t_reporting_user_history as (
select user_id, username, display_name, organization_name,
 user_type_id, user_type,is_internal,
is_navinet, can_create_realpa,is_verifiedNPI, address1,city,state,zip,email,phone,fax,
office_contact,created_on,deleted_on,eula_agree,referral_source,referral_type,software_vendor,account_id,
account_type_id,account_name, account_manager_user_id,group_name,user_category,user_bucket,user_agent,
npi,
ncpdp, first_touch,last_touch,requests,real_Requests,paplus_requests,real_requests_per_day,
paplus_requests_per_day,nn_officeid,user_branding_id,brand_name,
eula_accepted, last_eula_version_accepted, last_eula_accepted_date from field_demos.uc_lineage.t_reporting_user);

DROP TABLE IF EXISTS #verifiedNPI;

select distinct npi.user_id
INTO #verifiedNPI
from field_demos.uc_lineage.vw_VerifiedNPIs npi;

DROP TABLE IF EXISTS common.dbo.temp_t_reporting_user_staging;

SELECT
 u.id as user_id
 ,u.username
 ,u.display_name
 ,u.organization_name
 ,ut.id
 ,ut.user_type
 ,0 as is_internal
 ,0 as is_navinet
 ,ut.can_create_realpa
 ,npi.USER_ID npi_user_id
 ,u.prescriber_street
 ,u.prescriber_city
 ,u.prescriber_state
 ,u.prescriber_zip
 ,u.email
 ,u.office_phone
 ,u.office_fax
 ,u.office_contact
 ,u.created_on
 ,u.deleted 
 ,u.deleted_on
 ,u.eula_agree
 ,u.referral_source
 ,u.referral_type
 ,u.software_vendor
 ,u.account_id
 ,a.account_type_id
 ,a.name
 ,a.account_manager_user_id
 ,uc.description
 ,u.user_agent
 ,u.userbranding_id
 ,ub.brand_name
 ,ru.first_touch
 ,ru.last_touch
INTO common.dbo.temp_t_reporting_user_staging
FROM cmm_repl..t_users u
LEFT JOIN cmm_repl..t_usertype ut		ON u.user_type		 = ut.id
LEFT JOIN cmm_repl..t_usercategory uc	ON uc.id			 = ut.user_category_id
LEFT JOIN CMM_repl..T_UserBranding ub	ON u.userbranding_id = ub.id
LEFT JOIN cmm_repl..t_accounts a		ON u.account_id		 = a.id
LEFT JOIN #verifiedNPI npi				ON u.id				 = npi.user_id
LEFT join common.dbo.t_reporting_user ru ON ru.user_id = u.id;

truncate table field_demos.uc_lineage.t_reporting_user_merge;

insert into   field_demos.uc_lineage.t_reporting_user_merge
SELECT DISTINCT
  user_id
 ,username
 ,display_name
 ,CASE WHEN ISNULL(organization_name, '') = '' THEN 'Unknown' ELSE organization_name END
 ,id
 ,CASE WHEN ISNULL(REPLACE(user_type,'8','Retail Pharmacy'), '') = '' THEN 'Unknown' ELSE ISNULL(REPLACE(user_type,'8','Retail Pharmacy'), '')  END
 ,0 as is_internal
 ,0 as is_navinet
 ,ISNULL(can_create_realpa,0)
 ,CASE WHEN npi_USER_ID IS NOT NULL THEN 1 ELSE 0 END AS is_verifiedNPI
 ,CASE WHEN ISNULL(prescriber_street, '') = '' THEN 'Unknown' ELSE prescriber_street END as address1
 ,CASE WHEN ISNULL(prescriber_city, '') = '' THEN 'Unknown' ELSE prescriber_city END as prescriber_city
 ,CASE WHEN ISNULL(prescriber_state, '') = '' THEN 'Unknown' ELSE prescriber_state END
 ,isnull(nullif(common.dbo.RegexReplace(prescriber_zip, '[^\d]', ''), ''), 'Unknown') as zip
 ,CASE WHEN ISNULL(email, '') = '' THEN 'Unknown' ELSE email END
 ,isnull(nullif(common.dbo.RegexReplace(office_phone, '[^\d]', ''), ''), 'Unknown') as phone
 ,isnull(nullif(common.dbo.RegexReplace(office_fax, '[^\d]', ''), ''), 'Unknown') as fax
 ,office_contact
 ,created_on
 ,CASE WHEN deleted =1 THEN deleted_on ELSE NULL END as deleted_on
 ,eula_agree
 ,CASE WHEN ISNULL(referral_source, '') = '' THEN 'Unknown' ELSE referral_source END
 ,LTRIM(RTRIM(referral_type))
 ,LTRIM(RTRIM(software_vendor))
 ,account_id
 ,account_type_id
 ,name as Account_name
 ,account_manager_user_id
 ,NULL as group_name
 ,NULL as user_category
 ,description as user_bucket
 ,user_agent
 ,NULL
 ,NULL
 ,first_touch
 ,last_touch
 ,0
 ,0
 ,0
 ,NULL
 ,NULL
 ,NULL
 ,userbranding_id
 ,brand_name
 ,NULL
 ,NULL
 ,NULL
FROM common.dbo.temp_t_reporting_user_staging s;


DROP TABLE IF EXISTS common.dbo.temp_t_reporting_user_staging;

DROP TABLE IF EXISTS #verifiedNPI;


-- Run updates
IF OBJECT_ID('tempdb..#zipcodes') IS NOT NULL  DROP TABLE #zipcodes;

CREATE TABLE #zipcodes (zipcode VARCHAR(5), State VARCHAR(2));

INSERT #zipcodes
SELECT DISTINCT zip_cd as zipcode,
				state_cd  as state
FROM common.dbo.cmm_dim_geo
WHERE city_tp_cd = 'D';

CREATE INDEX idx_zip on #zipcodes(zipcode);

--update creator_state
UPDATE ru_new SET state = zc.state
      FROM t_reporting_user_merge ru_new
      JOIN #zipcodes zc
        ON LEFT(ru_new.zip,5) = zc.zipcode;
--30s

IF OBJECT_ID('tempdb..#internal') IS NOT NULL DROP TABLE #internal;

CREATE TABLE #internal (user_id INT, is_internal BIT)
;

with has_test_npi as (
	select 
		 ui.user_id
	from CMM_Repl..mtm_userIdentifier ui
	join CMM_Repl..t_identifier i
		on ui.identifier_id = i.id
	join CMM_Repl..t_identifiertype it
		on i.identifier_type = it.identifier_type
	where
		it.short_description = 'NPI'
		and i.identifier like replicate('[0-9]', 10)
		and i.identifier like '78787878%'
	group by ui.user_id
)
INSERT #internal
   SELECT u.id as user_id,
    CASE
	   WHEN
		 --the user is in the t_users_internal table
		 ui.id IS NOT NULL
		 OR  --the user's type is internal (If the type is unknown, default to 'non-internal')
		 ISNULL(ut.user_type,'Unknown') = 'Internal'
		 OR  --the user's email address is from CMM. (If the user/email is unknown, default to 'non-internal')
		 ISNULL(u.email,'Unknown') like '%@cmmtesting@'
		 OR  --the user's email address is from CMM. (If the user/email is unknown, default to 'non-internal')
		 ISNULL(u.email,'Unknown') like '%covermymeds_testing.com'
		 OR  --the user's email address is from CMM. (If the user/email is unknown, default to 'non-internal')
		 CASE WHEN e.email IS NOT NULL THEN 'EXTERNAL' ELSE ISNULL(u.email,'Unknown') END like '%covermymeds.com%'
		 OR  --the user's email address is from CMM. (If the user/email is unknown, default to 'non-internal')
		 ISNULL(u.email,'Unknown') like '%cmm_testing%'
		 OR  --the user's email address is from Innova. (If the user/email is unknown, default to 'non-internal')
		 ISNULL(u.email,'Unknown') like '%@innova-partners.com'
		 OR  --the user's email address is from Innova. (If the user/email is unknown, default to 'non-internal')
		 ISNULL(u.email,'Unknown') like '%@navinet.net'
		 OR  --the user's email address is from RelayHealth.
	   ISNULL(u.email,'Unknown') like '%@relayhealth.com'
		 OR
		 u.prescriber_street LIKE '%179%Lincoln%St%'
		 OR
		 u.prescriber_street LIKE '%130%chestnut%'
         OR  -- the user has a display name pattern used on internal test accounts
		 u.display_name LIKE '%functional test%'
		 OR 
		 u.username like '%rx_test_drugstore%' 
		OR 
		u.username = 'GroupingTestFixtureDummyUser'
		OR  --the user has an NPI pattern used on internal test accounts like '78787878%'
		 npi.user_id is not null 
        OR --these are test users DMSH-15401
        (u.username like 'mcstest%' and u.organization_name = 'Federal Pharmacies')
	   THEN 1
	   ELSE 0
    END as is_internal
  FROM cmm_repl..t_Users u
LEFT JOIN cmm_repl..t_Usertype ut			ON u.user_type = ut.id
LEFT JOIN cmm_repl..t_Users_Internal ui		ON u.id = ui.id
LEFT JOIN field_demos.uc_lineage.t_external_cmm_email e	ON u.email = e.email
LEFT JOIN has_test_npi npi					ON u.id = npi.user_id;

CREATE INDEX idxN_user_id on #internal(user_id);

--19s
UPDATE ru_new  SET ru_new.is_internal = i.is_internal
      FROM t_reporting_user_merge ru_new
      JOIN #internal i
        on ru_new.user_id = i.user_id;

 UPDATE ru_new SET ru_new.is_navinet = 1
      FROM t_reporting_user_merge ru_new
      JOIN vw_NaviNetUsers nn
        on ru_new.user_id = nn.user_id;

UPDATE ru_new  SET NN_officeID = id_nnid.identifier
      FROM t_reporting_user_merge ru_new
      JOIN CMM_repl..T_UserGroupMembership ugm
        ON ru_new.user_id = ugm.user_id
       AND ugm.role_id = 3
      JOIN CMM_repl..T_UserGroup ug
        ON ugm.group_id = ug.id
      JOIN CMM_repl..mtm_UserGroupIdentifier mtm_ugi_NNID
        ON ug.id = mtm_ugi_NNID.usergroup_id
      JOIN CMM_repl..T_Identifier id_NNID
        ON mtm_ugi_NNID.identifier_id = id_NNID.id
      JOIN CMM_repl..T_IdentifierType id_type_NNID
        ON id_type_NNID.identifier_type = id_NNID.identifier_type
       AND id_type_NNID.short_description like 'NNOfficeNID';


--Always work from last runtime 
--(do differently or not needed at all in databricks?)
DECLARE @MaxStartDate AS DATETIME = (
									SELECT TOP 1 CAST(StartTime as Date)
									FROM common.dbo.t_ReportingRefresh
									WHERE EndTime IS NOT NULL
									ORDER BY ID DESC
									);

DECLARE @MaxTouchID BIGINT = (
							SELECT TOP 1 MaxTouchID
							FROM common.dbo.t_ReportingRefresh
							WHERE StartTime >= DATEADD(DAY, -1, CAST(@MaxStartDate AS DATE))
							  AND StartTime < CAST(@MaxStartDate AS DATE)
							ORDER BY ID ASC
						    );

UPDATE ru_new 
SET last_touch = t.lasttouch,
   first_touch = CASE WHEN ru_new.first_touch IS NULL 
					  THEN t.firsttouch 
				 ELSE ru_new.first_touch
				 END
FROM  t_reporting_user_merge ru_new
JOIN (
	SELECT
		rt.user_id,
		MIN(rt.time) AS firsttouch
		,MAX(rt.time) as lasttouch
	FROM cmm_repl.dbo.t_requesttouches rt
	WHERE rt.id >= @MaxTouchID
	GROUP BY rt.user_id
	) t ON t.user_id = ru_new.user_id;

 --set npi/ncpdp
 UPDATE ru_new SET NPI = i.identifier
      FROM t_reporting_user_merge ru_new
      JOIN CMM_Repl..mtm_userIdentifier ui
        ON ru_new.user_id = ui.user_id
      JOIN CMM_Repl..t_identifier i
        ON ui.identifier_id = i.id
      JOIN CMM_Repl..t_identifiertype it
        ON i.identifier_type = it.identifier_type
     WHERE it.short_description = 'NPI'
        and i.identifier like replicate('[0-9]', 10);

UPDATE ru_new SET NCPDP = i.identifier
      FROM t_reporting_user_merge ru_new
      JOIN CMM_Repl..mtm_userIdentifier ui
        ON ru_new.user_id = ui.user_id
      JOIN CMM_Repl..t_identifier i
        ON ui.identifier_id = i.id
      JOIN CMM_Repl..t_identifiertype it
        ON i.identifier_type = it.identifier_type
     WHERE it.short_description = 'NCPDP';

 --set group_name
UPDATE ru_new SET group_name = ug.group_name
      FROM t_reporting_user_merge ru_new
      JOIN CMM_Repl..mtm_UserIdentifier ui
        ON ru_new.user_id = ui.user_id
      JOIN CMM_Repl..T_Identifier i
        ON ui.identifier_id = i.id
      JOIN CMM_Repl..T_IdentifierType it
        ON i.identifier_type = it.identifier_type
      JOIN cmm_repl..T_UserGroupMembership gm
        ON gm.user_id = ru_new.user_id
      JOIN cmm_repl..T_UserGroup ug
        ON gm.group_id = ug.id
      JOIN CMM_Repl..mtm_UserGroupIdentifier mgi
        ON mgi.usergroup_id = ug.id
      JOIN CMM_Repl..T_Identifier gi
        ON mgi.identifier_id = gi.id
      JOIN CMM_Repl..T_IdentifierType git
        ON gi.identifier_type = git.identifier_type
     WHERE it.short_description = 'NNUserNID'
       AND git.short_description = 'NNOfficeNID'
       AND gm.role_id = 3;

UPDATE ru_new SET user_category = CASE
     WHEN short_description IN ('NPI','NCPDP') THEN 'PriorAuthPlus'
     WHEN Short_Description LIKE 'NN%' THEN 'Navinet'
     WHEN Short_Description LIKE 'epoc%' THEN 'ePocrates'
     --ELSE 'Error'
 END
      FROM t_reporting_user_merge ru_new
      JOIN CMM_repl..mtm_UserIdentifier mtm_uid
        ON mtm_uid.user_id = ru_new.user_id
      JOIN CMM_repl..T_Identifier id
 ON id.id = mtm_uid.identifier_id
      JOIN CMM_repl..T_IdentifierType idt
        ON id.identifier_type = idt.identifier_type
     WHERE short_description NOT LIKE 'Migrated%';

--Update user_category
DROP TABLE IF EXISTS common.dbo.temp_user_rev_sources;

SELECT DISTINCT
	ru_new.user_id, 
	rr.revenue_source
INTO common.dbo.temp_user_rev_sources
FROM t_reporting_user_merge ru_new
JOIN t_reporting_request rr ON rr.creator_user_id = ru_new.user_id
AND rr.revenue_source IN ('EHR', 'LTC Integration', 'EasyButtonMsg', 'EasyButton','Pharmacy','Physician')
WHERE ru_new.user_category IS NULL;

DROP TABLE IF EXISTS common.dbo.temp_user_categories;

SELECT
	pt.user_id,
	CASE 1 WHEN pt.EHR THEN 'EHR'
		   WHEN pt.[LTC Integration] THEN 'LTC Integration'
		   WHEN pt.EasyButtonMsg THEN 'EasyButtonMsg'
		   WHEN pt.EasyButton THEN 'EasyButton'
		   WHEN pt.Pharmacy THEN 'Pharmacy'
		   WHEN pt.Physician THEN 'Physician'
	END as user_category
INTO common.dbo.temp_user_categories
FROM
(
	SELECT
		user_id,
		revenue_source,
		piv = 1
	FROM common.dbo.temp_user_rev_sources
) t
PIVOT(
COUNT(piv)
FOR revenue_source IN ([EHR], [LTC Integration], [EasyButtonMsg], [EasyButton],[Pharmacy],[Physician])
) AS pt;

DROP TABLE IF EXISTS common.dbo.temp_user_rev_sources;

UPDATE ru_new
SET user_category = uc.user_category
FROM common.dbo.t_reporting_user_merge ru_new
JOIN common.dbo.temp_user_categories uc ON uc.user_id = ru_new.user_id
WHERE ru_new.user_category IS NULL;

DROP TABLE IF EXISTS common.dbo.temp_user_categories;



UPDATE ru_new SET user_category = CASE WHEN user_type LIKE '%Pharmacy%' or user_type = '8' --prod bug 2015/10/21
       THEN 'Pharmacy' ELSE 'Physician' END
      FROM t_reporting_user_merge ru_new
     WHERE ru_new.user_category IS NULL;

--25s
UPDATE ru_new  SET referral_type = u.referral_type
      FROM t_reporting_user_merge ru_new
  JOIN CMM_Repl..T_Users u
        ON ru_new.user_id = u.id;

----10s
--UPDATE ru_new  SET referral_type = ISNULL(l.referral_type, 'Other')
--      FROM t_reporting_user_merge ru_new
--      JOIN CMM_Repl..t_users u
--        ON ru_new.user_id = u.id
-- LEFT JOIN CMM_Repl..t_referral_source_to_type l
--        ON ISNULL(u.referral_source,'') LIKE '%'+l.referral_source_keyword+'%'
--  WHERE ISNULL(ru_new.referral_type,'') = ''
												
UPDATE ru_new 
SET 
	requests		= requests.requests,
	real_requests	= requests.real_requests,
	PAPlus_requests = requests.PAPlus_requests
FROM t_reporting_user_merge ru_new
  JOIN (
    SELECT creator_user_id
           ,COUNT(*) as requests
           ,SUM(CAST(real as INT)) as real_requests
           ,SUM(CASE WHEN api_client LIKE 'RelayH%' THEN CAST(real as INT) ELSE 0 END) as PAPlus_requests
      FROM field_demos.uc_lineage.t_reporting_request rr
  GROUP BY creator_user_id
 ) requests
        ON ru_new.user_id  = requests.creator_user_id;

UPDATE ru_new SET real_requests_per_day = --select first_touch, real_requests,
 CAST(CASE
       WHEN DATEDIFF(d, ISNULL(first_touch, GETDATE()), GETDATE()) = 0 THEN real_requests
       ELSE CAST(real_requests as DECIMAL) / DATEDIFF(d, first_touch, GETDATE())
     END as  NUMERIC(8,4))
 ,PAPlus_requests_per_day =
  CAST(CASE
       WHEN DATEDIFF(d, ISNULL(first_touch, GETDATE()), GETDATE()) = 0 THEN PAPlus_requests
       ELSE CAST(PAPlus_requests as DECIMAL) / DATEDIFF(d, first_touch, GETDATE())
    END as  NUMERIC(8,4))
       FROM t_reporting_user_merge ru_new;

UPDATE ru_new SET ru_new.NPI = n.npi
     FROM t_reporting_user_merge ru_new
     JOIN t_dba_userid_NPI n
       ON ru_new.user_id = n.user_id
    WHERE ru_new.NPI IS NULL
       OR LEN(ru_new.npi)<10;

drop table if exists #eulas;

select
    USER_ID
    ,MAX(created_at at time zone 'UTC' at time zone 'Eastern Standard Time') as last_eula_accepted_date
    ,MAX(eula_version_id) as last_eula_version_accepted
into #eulas
from eulas_repl.dbo.eula_version_actions
where action_id=2
group by USER_ID;

--17s
UPDATE tru
SET eula_accepted = 1, last_eula_version_accepted = e.last_eula_version_accepted,
    last_eula_accepted_date = e.last_eula_accepted_date
-- SELECT COUNT(*)
FROM field_demos.uc_lineage.t_reporting_user_merge tru
JOIN #eulas e
    ON e.user_id = tru.user_id;

--update sponsored renewal related records that have an "unknown" org name
update tru
set
    tru.organization_name = 'CoverMyMeds Sponsored Renewals'
from
    common.dbo.t_reporting_user_merge tru
where
    display_name like '%sponsored renewals%'
    and organization_name = 'Unknown';

update statistics t_reporting_user_merge;
-- Begin Merge

TRUNCATE TABLE common.dbo.t_reporting_user_changes;

DECLARE @action_date DATETIME = getdate();

MERGE field_demos.uc_lineage.t_reporting_user WITH (HOLDLOCK) AS T
USING field_demos.uc_lineage.t_reporting_user_merge AS S
ON (T.user_id = S.user_id)
WHEN NOT MATCHED BY TARGET
THEN INSERT(user_id, username, display_name, organization_name, user_type_id, user_type,is_internal,
is_navinet, can_create_realpa,is_verifiedNPI, address1,city,state,zip,email,phone,fax,
office_contact,created_on,deleted_on,eula_agree,referral_source,referral_type,software_vendor,account_id,
account_type_id,account_name, account_manager_user_id,group_name,user_category,user_bucket,npi,
ncpdp, first_touch,last_touch,requests,real_Requests,paplus_requests,real_requests_per_day,
paplus_requests_per_day,nn_officeid,user_branding_id,brand_name, eula_accepted, last_eula_version_accepted,
last_eula_accepted_date)
VALUES(s.user_id, s.username, s.display_name, s.organization_name, s.user_type_id, s.user_type,s.is_internal,
s.is_navinet, s.can_create_realpa,s.is_verifiedNPI, s.address1,s.city,s.state,s.zip,s.email,s.phone,s.fax,
s.office_contact,s.created_on,s.deleted_on,s.eula_agree,s.referral_source,s.referral_type,s.software_vendor,s.account_id,
s.account_type_id,s.account_name,s.account_manager_user_id,s.group_name,user_category,s.user_bucket,s.npi,
s.ncpdp, s.first_touch,s.last_touch,s.requests,s.real_Requests,s.paplus_requests,s.real_requests_per_day,
s.paplus_requests_per_day,s.nn_officeid,s.user_branding_id,s.brand_name, s.eula_accepted, s.last_eula_version_accepted,
s.last_eula_accepted_date)
WHEN MATCHED
AND ( 
   ISNULL(T.username,'')						<> ISNULL(S.username,'')
OR ISNULL(T.display_name,'')					<> ISNULL(S.display_name,'')
OR ISNULL(T.organization_name,'')				<> ISNULL(S.organization_name,'')
OR ISNULL(T.user_type_id,'')					<> ISNULL(S.user_type_id,'')
OR ISNULL(T.user_type,'')						<> ISNULL(S.user_type,'')
OR ISNULL(T.is_internal,'')						<> ISNULL(S.is_internal,'')
OR ISNULL(T.is_navinet,'')						<> ISNULL(S.is_navinet,'')
OR ISNULL(T.can_create_realpa,'')				<> ISNULL(S.can_create_realpa,'')
OR ISNULL(T.is_verifiedNPI,'')					<> ISNULL(S.is_verifiedNPI,'')
OR ISNULL(T.address1,'')						<> ISNULL(S.address1,'')
OR ISNULL(T.city,'')							<> ISNULL(S.city,'')
OR ISNULL(T.state,'')							<> ISNULL(S.state,'')
OR ISNULL(T.zip,'')								<> ISNULL(S.zip,'')
OR ISNULL(T.email,'')							<> ISNULL(S.email,'')
OR ISNULL(T.phone,'')							<> ISNULL(S.phone,'')
OR ISNULL(T.fax,'')								<> ISNULL(S.fax,'')
OR ISNULL(T.office_contact,'')					<> ISNULL(S.office_contact,'')
OR ISNULL(T.created_on,'')						<> ISNULL(S.created_on,'')
OR ISNULL(T.deleted_on,'')						<> ISNULL(S.deleted_on,'')
OR ISNULL(T.eula_agree,'')						<> ISNULL(S.eula_agree,'')
OR ISNULL(T.referral_source,'')					<> ISNULL(S.referral_source,'')
OR ISNULL(T.referral_type,'')					<> ISNULL(S.referral_type,'')
OR ISNULL(T.software_vendor,'')					<> ISNULL(S.software_vendor,'')
OR ISNULL(T.account_id,'')						<> ISNULL(S.account_id,'')
OR ISNULL(T.account_type_id,'')					<> ISNULL(S.account_type_id,'')
OR ISNULL(T.account_name,'')					<> ISNULL(S.account_name,'')
OR ISNULL(T.account_manager_user_id,'')			<> ISNULL(S.account_manager_user_id,'')
OR ISNULL(T.group_name,'')						<> ISNULL(S.group_name,'')
OR ISNULL(T.user_category,'')					<> ISNULL(S.user_category,'')
OR ISNULL(T.user_bucket,'')						<> ISNULL(S.user_bucket,'')
OR ISNULL(T.npi,'')								<> ISNULL(S.npi,'')
OR ISNULL(T.ncpdp,'')							<> ISNULL(S.ncpdp,'')
OR ISNULL(T.first_touch,'')						<> ISNULL(S.first_touch,'')
OR ISNULL(T.last_touch,'')						<> ISNULL(S.last_touch,'') 
OR ISNULL(T.requests,'')						<> ISNULL(S.requests,'')
OR ISNULL(T.real_Requests,'')					<> ISNULL(S.real_Requests,'')
OR ISNULL(T.paplus_requests,'')					<> ISNULL(S.paplus_requests,'')
OR ISNULL(T.real_requests_per_day,'')			<> ISNULL(S.real_requests_per_day,'')
OR ISNULL(T.paplus_requests_per_day,'')			<> ISNULL(S.paplus_requests_per_day,'')
OR ISNULL(T.nn_officeid,'')						<> ISNULL(S.nn_officeid,'')
OR ISNULL(T.user_branding_id,'')				<> ISNULL(S.user_branding_id,'')
OR ISNULL(T.brand_name,'')						<> ISNULL(S.brand_name,'') 
OR ISNULL(t.eula_accepted,'')					<> ISNULL(s.eula_accepted,'')
OR ISNULL(t.last_eula_version_accepted,'')		<> ISNULL(s.last_eula_version_accepted,'') 
OR ISNULL(t.last_eula_accepted_date,'')			<> ISNULL(s.last_eula_accepted_date,'')
)

THEN UPDATE SET
--T.user_id = S.user_id, 
T.username = S.username,
T.display_name = S.display_name,
T.organization_name = S.organization_name,
T.user_type_id = S.user_type_id,
T.user_type = S.user_type,
T.is_internal = S.is_internal,
T.is_navinet = S.is_navinet,
T.can_create_realpa = S.can_create_realpa,
T.is_verifiedNPI = S.is_verifiedNPI,
T.address1 = S.address1,
T.city = S.city,
T.state = S.state,
T.zip = S.zip,
T.email = S.email,
T.phone = S.phone,
T.fax = S.fax,
T.office_contact = S.office_contact,
T.created_on = S.created_on,
T.deleted_on = S.deleted_on,
T.eula_agree = S.eula_agree,
T.referral_source = S.referral_source,
T.referral_type = S.referral_type,
T.software_vendor = S.software_vendor,
T.account_id = S.account_id,
T.account_type_id = S.account_type_id,
T.account_name = S.account_name,
T.account_manager_user_id = S.account_manager_user_id,
T.group_name = S.group_name,
T.user_category = S.user_category,
T.user_bucket = S.user_bucket,
T.npi = S.npi,
T.ncpdp = S.ncpdp,
T.first_touch = S.first_touch,
T.last_touch = S.last_touch, 
T.requests = S.requests,
T.real_Requests = S.real_Requests,
T.paplus_requests = S.paplus_requests,
T.real_requests_per_day = S.real_requests_per_day,
T.paplus_requests_per_day = S.paplus_requests_per_day,
T.nn_officeid = S.nn_officeid,
T.user_branding_id = S.user_branding_id,
T.brand_name  = S.brand_name, 
t.eula_accepted = s.eula_accepted,
t.last_eula_version_accepted = s.last_eula_version_accepted, 
t.last_eula_accepted_date = s.last_eula_accepted_date;