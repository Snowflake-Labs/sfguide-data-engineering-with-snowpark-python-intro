CREATE OR REPLACE VIEW v_department_summary AS
SELECT
    d.department_id,
    d.department_name,
    d.manager_id,
    l.city AS department_location,
    c.country_name,
    r.region_name,
    COUNT(e.employee_id) AS total_employees,
    AVG(e.salary) AS avg_salary,
    LISTAGG(j.job_title, ', ') WITHIN GROUP (ORDER BY j.job_title) AS job_titles
FROM
    employees e
JOIN
    departments d ON e.department_id = d.department_id
JOIN
    locations l ON d.location_id = l.location_id
JOIN
    countries c ON l.country_id = c.country_id
JOIN
    regions r ON c.region_id = r.region_id
JOIN
    jobs j ON e.job_id = j.job_id
GROUP BY
    d.department_id, d.department_name, d.manager_id, l.city, c.country_name, r.region_name
ORDER BY
    d.department_name;
