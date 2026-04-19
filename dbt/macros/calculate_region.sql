{%- macro calculate_region(state_column) -%}
    CASE
        WHEN {{ state_column }} IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'Northeast'
        WHEN {{ state_column }} IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'Midwest'
        WHEN {{ state_column }} IN ('DE','FL','GA','MD','NC','SC','VA','WV','AL','KY','MS','TN','AR','LA','OK','TX','DC') THEN 'South'
        WHEN {{ state_column }} IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'West'
        WHEN {{ state_column }} IN ('AS','GU','MP','PR','VI','UM','FM','MH','PW') THEN 'Territories'
        ELSE 'Unknown'
    END
{%- endmacro -%}
