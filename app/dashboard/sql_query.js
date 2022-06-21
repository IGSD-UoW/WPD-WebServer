const sql_query = module.exports


// ===============
// 01. SEARCH
// ===============

/**
 * @param {string} searchValue
 * @returns {string}
 */
sql_query.search = (searchValue) => {



    return `
    select array_to_json(array_agg(foo)) from (
    select * from mat_view_search where placename ilike '${searchValue}%' limit 5
    ) foo
    `
}

// ===============
// 02. SIMPLE GEOMETRY
// ===============

/**
 * @param {number} locationID
 * @returns {string}
 */

sql_query.simpleGeometry = (locationID) => {
    console.log('>>> simple geometry ')

    return `
    select array_to_json(array_agg(formsagg))
    from (
    
    select f.id, aid.value placeid, an.value placename, atp.value placetype, 
    ST_X(ST_Centroid(ST_Transform(f.geom::geometry, 4326))) longitude,
    ST_Y(ST_Centroid(ST_Transform(f.geom::geometry, 4326))) latitude
    from formsanswers f, fieldsanswers aid, fieldsanswers an, fieldsanswers atp
    where f.id = aid.idformsanswers and
    f.id = an.idformsanswers and
    f.id = atp.idformsanswers and
    f.idforms = 12 and
    aid.idfields = 82 and
    an.idfields = 83 and
    atp.idfields = 84 and
    aid.value like '${locationID}'
    
    )
    formsagg
    `
}

// ===============
// 03. PLUVIOMETER RECORDS
// ===============

/**
 * @param {number} locationID
 * @returns {string}
 */

sql_query.pluviometerRecords = (locationID, startDate, endDate) => {
    console.log('>>> pluviometer records ')

    return `
        select array_to_json(array_agg(formsagg))
        from (
	    select fs.latitude latitude, fs.longitude longitude,
		'C' as submissiontype, u.institutiontype institutionname, u.institution institutioninfo,
		( 	
		select array_to_json(array_agg(meagg))
	  	from (
				select am.value, am.dtfilling as timestamp
				from formsanswers fm, fieldsanswers am
				where fm.id = am.idformsanswers and
				fm.idforms = 8 and
				am.idfields = 59 and
				fm.idusersinformer = fs.idusersinformer and
				extract(epoch from am.dtfilling) * 1000 between '${startDate}' and '${endDate}'
				order by am.dtfilling
		) meagg ) as records
        from formsanswers fs, formsanswers fp, fieldsanswers ap, auth.users u
        where fp.id = ap.idformsanswers and
        fs.idusersinformer = u.id and
        fs.idusersinformer != 1 and
        fs.idforms = 7 and
        fp.idforms = 12 and
        ap.idfields = 82 and
        ap.value like '${locationID}' and
        st_intersects(fp.geom, fs.geom) and
        ( 	
	    select count(am.value)
				from formsanswers fm, fieldsanswers am
				where fm.id = am.idformsanswers and
					fm.idforms = 8 and
					am.idfields = 59 and
					fm.idusersinformer = fs.idusersinformer and
				extract(epoch from am.dtfilling) * 1000 between '${startDate}' and '${endDate}'
		    ) > 0
        ) 
        formsagg;
    `
}


// ===============
// 04. SUMMARY
// ===============

/**
 * @returns {string}
 */

sql_query.summary = () => {
    console.log('>>> summary ')

    return `
        select array_to_json(array_agg(formsagg))
        from (select (select count(distinct (fp.idusersinformer))
                      from formsanswers fp
                      where fp.idforms = 7
                        and fp.idusersinformer != 1) pluviometers,
             (select count(fr.id)
              from formsanswers fr
              where fr.idforms in (7, 8, 9, 10, 11)) rowsOfData,
             (select count(distinct (fre.idusersinformer))
              from formsanswers fre
              where fre.idforms in (7, 8, 9, 10, 11)
                and fre.idusersinformer != 1) citizenReporters,
             (select count(distinct (u.institution))
              from formsanswers fsc,
                   auth.users u
              where fsc.idusersinformer = u.id
                and fsc.idforms in (7, 8, 9, 10, 11)
                and fsc.idusersinformer != 1 and u.institutiontype = 'E') partnerSchools,
             (select count(distinct (u.institution))
              from formsanswers fsc,
                   auth.users u
              where fsc.idusersinformer = u.id
                and fsc.idforms in (7, 8, 9, 10, 11)
                and fsc.idusersinformer != 1 and u.institutiontype = 'D') civilDefenseAgencies -- Name changed

            ) formsagg
    `
}



// ===============
// 06. CITIZEN REPORTS OVERVIEW
// ===============

/**
 * @param {number} startDate
 * @param {number} endDate
 * @returns {string}
 */

sql_query.citizenReportsOverview = (startDate, endDate) => {
    console.log('>>> citizen reports overview ')

    return `
        select array_to_json(array_agg(formsagg))
          from (
           
            select f.latitude, f.longitude, f.id
            from formsanswers f, fieldsanswers fa
            where fa.idformsanswers = f.id and
            f.idusersinformer != 1 and
            f.idforms in (7, 8, 9, 10, 11) 
            group by f.latitude, f.longitude, f.id
            having extract(epoch from max(fa.dtfilling)) * 1000 between '${startDate}' and '${endDate}'
            
        ) formsagg;
    `
}

// ===============
// 07. AVG RAINFALL OVERVIEW
// ===============

/**
 * @param {number} startDate
 * @param {number} endDate
 * @returns {string}
 */

sql_query.avgRainfallOverview = (startDate, endDate) => {
    console.log('>>> avg rainfall overview ')

    return `
        select array_to_json(array_agg(formsagg))
        from (select f.latitude, f.longitude, count(fa.value) countreport, avg(fa.value::float) avgrainreport
              from formsanswers f,
                   fieldsanswers fa
              where fa.idformsanswers = f.id
                and fa.idfields = 59
                and f.idusersinformer != 1 and
                \tf.idforms = 8
              group by f.latitude, f.longitude
              having extract (epoch from max (fa.dtfilling)) * 1000 between '${startDate}' and '${endDate}') formsagg;
    `
}


// ===============
// 08. CITIZEN EVENTS ENDPOINT
// ===============

/**
 * @param {number} id
 * @param {number} type
 * @param {number} startDate
 * @param {number} endDate
 * @returns {string}
 */

sql_query.citizenEvents = (locationID, formType, startDate, endDate) => {
    console.log('>>> citizen events ')

    return `
        select array_to_json(array_agg(formsagg))
 from (
  
	select fsub.id submissionid, fsub.latitude, fsub.longitude, asub.value submissiontext,
		asub.dtfilling submissiontimestamp, f.code submissiontype, 
		u.institutiontype organisationtype, apln.value locationame, aplt.value locationtype
	from formsanswers fpl, fieldsanswers apl, fieldsanswers apln, fieldsanswers aplt, 
		formsanswers fsub, fieldsanswers asub, forms f,
		auth.users u 
	where fpl.id = apl.idformsanswers and
	fpl.id = apln.idformsanswers and
	fpl.id = aplt.idformsanswers and
	fsub.id = asub.idformsanswers and
	fsub.idforms = f.id and
	fsub.idusersinformer = u.id and
	fpl.idforms = 12 and
	apl.idfields = 82 and
	apln.idfields = 83 and
	aplt.idfields = 84 and
	fsub.idforms = '${formType}' and -- parameter 
	asub.idfields = CASE WHEN fsub.idforms = 9 THEN 66
   WHEN fsub.idforms = 10 THEN 71
   WHEN fsub.idforms = 11 then 76 END and -- case based on previous parameter
	apl.value like '${locationID}' and 
	extract(epoch from asub.dtfilling) * 1000 between '${startDate}' and '${endDate}' and -- parameter
	st_intersects(fpl.geom, fsub.geom)  -- TBI case based on placetype
	order by fsub.id
	
) formsagg;
    `
}


// ===============
// 09. PLACE SUMMARY ENDPOINT
// ===============

/**
 * @param {number} id
 * @param {number} startDate
 * @param {number} endDate
 * @returns {string}
 */

sql_query.placeSummary = (locationID, startDate, endDate) => {
    console.log('>>> place summary ')

    return `
        select array_to_json(array_agg(formsagg))
 from (
	
 	select  
	(select count(distinct(ff.idusersinformer)) 
	from formsanswers ff, fieldsanswers af, formsanswers fpl, fieldsanswers apl
	where fpl.id = apl.idformsanswers and ff.id = af.idformsanswers and
	ff.idforms in (10, 11) and fpl.idforms = 12 and
	apl.idfields = 82 and
	ff.idusersinformer != 1 and
	apl.value like '${locationID}' and -- Parameter
	extract(epoch from af.dtfilling) * 1000 between 1647718000000 and 1655310000000 and -- Parameters 
	st_intersects(ff.geom, fpl.geom)) floodReports, 
	(select avg(ar.value::float) 
	from formsanswers fr, fieldsanswers ar, formsanswers fpl, fieldsanswers apl
	where fpl.id = apl.idformsanswers and ar.idformsanswers = fr.id and
	ar.idfields = 59 and fr.idforms = 8 and
	apl.idfields = 82 and fpl.idforms = 12 and
	fr.idusersinformer != 1 and
	apl.value like '${locationID}' and -- Parameter
	extract(epoch from ar.dtfilling) * 1000 between '${startDate}' and '${endDate}' and -- Parameters 
	st_intersects(fr.geom, fpl.geom)) avgdailyrainfall,
	( select 11
	) citizenReporters
	
) formsagg;
    `
}


// ===============
// 05. FLOOD ZONES
// ===============

/**
 * @param {number} locationID
 * @returns {string}
 */

sql_query.floodZones = (locationID) => {
    console.log('>>> floodZones')

    return `
    with features as (
    select array_to_json(array_agg(t)) from (
    select
    'Feature' as "type",
    row_number() over () id,
    fzon.id zoneid,
    acl.value classvalue,
    ayr.value yearvalue,
    'O' as submissiontype,
    st_asgeojson(fzon.geom,6)::json as "geometry"
    from formsanswers fpl, fieldsanswers apl, formsanswers fzon, fieldsanswers acl, fieldsanswers ayr
    where fpl.id = apl.idformsanswers and
    fzon.id = acl.idformsanswers and
    fzon.id = ayr.idformsanswers and
    fzon.idforms = 4 and
    fpl.idforms = 12 and
    acl.idfields = 24 and
    ayr.idfields = 25 and
    apl.idfields = 82 and
    apl.value like '${locationID}' and
    st_intersects(fpl.geom, fzon.geom)
    order by fzon.id limit 500) as t )
    select json_build_object(
    'type', 'FeatureCollection',
    'features', (features.*)
    ) from features
    `
}


// ===============
// 10. FLOODZONES BY BBOX
// ===============

/**
 * Query to get all floodzones for the given spatial point with its buffer range and its fieldAnswers (attributes)
 * belonging to a specific forms.code
 *
 * @param {comma-seperated-numbers} bbox
 * @param {string} db_schema
 * @returns {string}
 */
sql_query.floodZonesByBBOX = (bbox, db_schema, userSchema) => {
    console.log('>>> dataByBbox ')


    return `
    with features as (
select array_to_json(array_agg(fa))
from
(
  select
    fa.id as formsanswersid,
    acl.value classvalue,
    'O' as submissiontype,
    ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom::geometry, 0.0001))::json as geometry,
    'Feature' as type
  from ${db_schema}.formsanswers fa
  inner join ${db_schema}.forms f on (fa.idforms = f.id )
  inner join ${db_schema}.fieldsanswers acl on (acl.idformsanswers = fa.id)
  where 1=1
    and ST_Intersects(fa.geom, ST_MakeEnvelope(${bbox}, 4326))
    and f.code = 'FLOODZONES_OFFICIAL'
    and acl.idfields = 24
  limit 500
) fa)
select json_build_object(
'type', 'FeatureCollection',
'features', (features.*)
) from features
    `
}