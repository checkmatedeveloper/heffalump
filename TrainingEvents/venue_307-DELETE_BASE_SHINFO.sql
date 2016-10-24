DELETE 
                   FROM info.unit_patron_discounts
                   WHERE unit_patron_uid IN (SELECT
                                                unit_x_patrons.id
                                             FROM info.unit_x_patrons
                                             JOIN setup.units ON unit_x_patrons.unit_uid = units.id
                                             JOIN patrons.clone_patrons ON clone_patrons.id = unit_x_patrons.patron_uid
                                             WHERE venue_uid = 307 AND patron_uid IN (62484, 62485, 62486, 62487, 62488, 62489, 62490));
                   DELETE 
                   FROM info.unit_patron_gratuities
                   WHERE unit_patron_uid IN (SELECT
                                                unit_x_patrons.id
                                             FROM info.unit_x_patrons
                                             JOIN setup.units ON unit_x_patrons.unit_uid = units.id
                                             JOIN patrons.clone_patrons ON clone_patrons.id = unit_x_patrons.patron_uid
                                             WHERE venue_uid = 307 AND patron_uid IN (62484, 62485, 62486, 62487, 62488, 62489, 62490));
                   DELETE 
                   FROM info.unit_patron_info
                   WHERE unit_patron_uid IN (SELECT
                                                unit_x_patrons.id
                                             FROM info.unit_x_patrons
                                             JOIN setup.units ON unit_x_patrons.unit_uid = units.id
                                             JOIN patrons.clone_patrons ON clone_patrons.id = unit_x_patrons.patron_uid
                                             WHERE venue_uid = 307 AND patron_uid IN (62484, 62485, 62486, 62487, 62488, 62489, 62490));
                   DELETE 
                   FROM info.unit_patron_notes
                   WHERE unit_patron_uid IN (SELECT
                                                unit_x_patrons.id
                                             FROM info.unit_x_patrons
                                             JOIN setup.units ON unit_x_patrons.unit_uid = units.id
                                             JOIN patrons.clone_patrons ON clone_patrons.id = unit_x_patrons.patron_uid
                                             WHERE venue_uid = 307 AND patron_uid IN (62484, 62485, 62486, 62487, 62488, 62489, 62490));
                   DELETE 
                   FROM info.unit_patron_pars
                   WHERE unit_patron_uid IN (SELECT
                                                unit_x_patrons.id
                                             FROM info.unit_x_patrons
                                             JOIN setup.units ON unit_x_patrons.unit_uid = units.id
                                             JOIN patrons.clone_patrons ON clone_patrons.id = unit_x_patrons.patron_uid
                                             WHERE venue_uid = 307 AND patron_uid IN (62484, 62485, 62486, 62487, 62488, 62489, 62490));