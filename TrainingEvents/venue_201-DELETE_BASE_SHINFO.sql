DELETE
FROM info.unit_patron_discounts
WHERE unit_patron_uid IN (SELECT
							unit_x_patrons.id
						  FROM info.unit_x_patrons
						  JOIN setup.units ON unit_x_patrons.unit_uid = units.id
						  JOIN patrons.clone_patrons ON clone_patrons.id = unit_x_patrons.patron_uid
						  WHERE venue_uid = 201 AND patron_uid IN (36737, 36738, 36739, 36740, 36741, 36742, 36743));

DELETE
FROM info.unit_patron_gratuities
WHERE unit_patron_uid IN (SELECT
							unit_x_patrons.id
						  FROM info.unit_x_patrons
						  JOIN setup.units ON unit_x_patrons.unit_uid = units.id
						  JOIN patrons.clone_patrons ON clone_patrons.id = unit_x_patrons.patron_uid
						  WHERE venue_uid = 201 AND patron_uid IN (36737, 36738, 36739, 36740, 36741, 36742, 36743));

DELETE
FROM info.unit_patron_info
WHERE unit_patron_uid IN (SELECT
							unit_x_patrons.id
						  FROM info.unit_x_patrons
						  JOIN setup.units ON unit_x_patrons.unit_uid = units.id
						  JOIN patrons.clone_patrons ON clone_patrons.id = unit_x_patrons.patron_uid
						  WHERE venue_uid = 201 AND patron_uid IN (36737, 36738, 36739, 36740, 36741, 36742, 36743));

DELETE
FROM info.unit_patron_notes
WHERE unit_patron_uid IN (SELECT
							unit_x_patrons.id
						  FROM info.unit_x_patrons
						  JOIN setup.units ON unit_x_patrons.unit_uid = units.id
						  JOIN patrons.clone_patrons ON clone_patrons.id = unit_x_patrons.patron_uid
						  WHERE venue_uid = 201 AND patron_uid IN (36737, 36738, 36739, 36740, 36741, 36742, 36743));


DELETE
FROM info.unit_patron_pars
WHERE unit_patron_uid IN (SELECT
							unit_x_patrons.id
						  FROM info.unit_x_patrons
						  JOIN setup.units ON unit_x_patrons.unit_uid = units.id
						  JOIN patrons.clone_patrons ON clone_patrons.id = unit_x_patrons.patron_uid
						  WHERE venue_uid = 201 AND patron_uid IN (36737, 36738, 36739, 36740, 36741, 36742, 36743));
