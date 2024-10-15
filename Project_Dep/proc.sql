CREATE OR REPLACE PROCEDURE get_department_summary(
    p_department_id IN NUMBER DEFAULT NULL
) IS
BEGIN
    -- Fetch department summary based on the optional parameter
    IF p_department_id IS NULL THEN
        -- If no department ID is provided, show for all departments
        FOR dept_rec IN (
            SELECT * FROM v_department_summary
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('Department ID: ' || dept_rec.department_id);
            DBMS_OUTPUT.PUT_LINE('Department Name: ' || dept_rec.department_name);
            DBMS_OUTPUT.PUT_LINE('Manager ID: ' || dept_rec.manager_id);
            DBMS_OUTPUT.PUT_LINE('Location: ' || dept_rec.department_location);
            DBMS_OUTPUT.PUT_LINE('Country: ' || dept_rec.country_name);
            DBMS_OUTPUT.PUT_LINE('Region: ' || dept_rec.region_name);
            DBMS_OUTPUT.PUT_LINE('Total Employees: ' || dept_rec.total_employees);
            DBMS_OUTPUT.PUT_LINE('Average Salary: ' || dept_rec.avg_salary);
            DBMS_OUTPUT.PUT_LINE('Job Titles: ' || dept_rec.job_titles);
            DBMS_OUTPUT.PUT_LINE('-----------------------------------------');
        END LOOP;
    ELSE
        -- If a specific department ID is provided, show details for that department only
        FOR dept_rec IN (
            SELECT * FROM v_department_summary
            WHERE department_id = p_department_id
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('Department ID: ' || dept_rec.department_id);
            DBMS_OUTPUT.PUT_LINE('Department Name: ' || dept_rec.department_name);
            DBMS_OUTPUT.PUT_LINE('Manager ID: ' || dept_rec.manager_id);
            DBMS_OUTPUT.PUT_LINE('Location: ' || dept_rec.department_location);
            DBMS_OUTPUT.PUT_LINE('Country: ' || dept_rec.country_name);
            DBMS_OUTPUT.PUT_LINE('Region: ' || dept_rec.region_name);
            DBMS_OUTPUT.PUT_LINE('Total Employees: ' || dept_rec.total_employees);
            DBMS_OUTPUT.PUT_LINE('Average Salary: ' || dept_rec.avg_salary);
            DBMS_OUTPUT.PUT_LINE('Job Titles: ' || dept_rec.job_titles);
            DBMS_OUTPUT.PUT_LINE('-----------------------------------------');
        END LOOP;
    END IF;
END get_department_summary;

