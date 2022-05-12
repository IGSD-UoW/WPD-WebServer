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
    select array_to_json(array_agg(formsagg))
    from (
    
    
    select f.id, aid.value placeid, an.value placename, atp.value placetype
    from formsanswers f, fieldsanswers aid, fieldsanswers an, fieldsanswers atp
    where f.id = aid.idformsanswers and
    f.id = an.idformsanswers and
    f.id = atp.idformsanswers and
    f.idforms = 12 and
    aid.idfields = 82 and
    an.idfields = 83 and
    atp.idfields = 84 and
    an.value like '${searchValue}%'
    limit 5
    )
    formsagg
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
    aid.value like '${locationID}%'
    
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

    console.log('Start Date', startDate)
    console.log('End Date', endDate)

    return `
        select array_to_json(array_agg(formsagg))
        from (
	    select fs.latitude latitude, fs.longitude logitude, 
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
        ap.value like '${locationID}%' and 
        st_intersects(fp.geom, fs.geom)
        ) 
        formsagg;
    `
}
