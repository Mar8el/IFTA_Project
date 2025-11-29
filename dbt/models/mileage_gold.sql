

{{ config(
    materialized='table'
) }}




SELECT
    t."State",
    t.Mileage
FROM
    {{ ref('mileage_silver') }} AS m
    CROSS JOIN LATERAL (
        VALUES
            (m.al, 'AL'),
            (m.ar, 'AR'),
            (m.az, 'AZ'),
            (m.CA, 'CA'),
            (m.CO, 'CO'),
            (m.CT, 'CT'),
            (m.DE, 'DE'),
            (m.FL, 'FL'),
            (m.GA, 'GA'),
            (m.ID, 'ID'),
            (m.IL, 'IL'),
            (m.INDIANA, 'IN'),
            (m.IA, 'IA'),
            (m.KS, 'KS'),
            (m.KY, 'KY'),
            (m.LA, 'LA'),
            (m.ME, 'ME'),
            (m.MD, 'MD'),
            (m.MA, 'MA'),
            (m.MI, 'MI'),
            (m.MN, 'MN'),
            (m.MS, 'MS'),
            (m.MO, 'MO'),
            (m.MT, 'MT'),
            (m.NE, 'NE'),
            (m.NV, 'NV'),
            (m.NH, 'NH'),
            (m.NJ, 'NJ'),
            (m.NM, 'NM'),
            (m.NY, 'NY'),
            (m.NC, 'NC'),
            (m.ND, 'ND'),
            (m.OH, 'OH'),
            (m.OK, 'OK'),
            (m.OREGON, 'OR'),
            (m.PA, 'PA'),
            (m.RI, 'RI'),
            (m.SC, 'SC'),
            (m.SD, 'SD'),
            (m.TN, 'TN'),
            (m.TX, 'TX'),
            (m.UT, 'UT'),
            (m.VT, 'VT'),
            (m.VA, 'VA'),
            (m.WA, 'WA'),
            (m.WV, 'WV'),
            (m.WI, 'WI'),
            (m.WY, 'WY')
            
    ) AS t(Mileage, "State")