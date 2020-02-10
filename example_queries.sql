SELECT DATE_TRUNC(issue_date, WEEK) as Week, count(distinct CONCAT( state, ":", plate )) as NumberVehicles, count(*) as NumberViolations
FROM `jehiah-p1.traffic_violations_311.open_parking_and_camera_violations_nc67_uf89` 
WHERE issue_date >= "2018-09-21" 
AND violation = 'PHTO SCHOOL ZN SPEED VIOLATION'
GROUP by 1
order by 1 desc



-- Number of Violations by Day
SELECT issue_date, count(distinct CONCAT( state, ":", plate )) as Count
FROM `jehiah-p1.traffic_violations_311.open_parking_and_camera_violations_nc67_uf89` 
WHERE issue_date >= "2018-09-21" 
AND violation = 'PHTO SCHOOL ZN SPEED VIOLATION'
GROUP by 1
order by 1 desc


-- The Drivers grouped by the number of violations since June
SELECT NumberViolations, count(*) as NumberDrivers
FROM (
    SELECT concat(state, ":", plate) as Vehicle, count(*) as NumberViolations
    FROM `jehiah-p1.traffic_violations_311.open_parking_and_camera_violations_nc67_uf89` 
    WHERE VIOLATION = 'PHTO SCHOOL ZN SPEED VIOLATION'
    AND issue_date between '2019-06-01' and '2019-11-01'
    GROUP BY 1
) 
GROUP BY 1 ORDER BY 1 asc



-- Frequency of Violation
SELECT issue_date, ViolationNumber, count(*) as NumberViolations
FROM  (
    SELECT issue_date,
    RANK() OVER ( PARTITION BY CONCAT(state, ":", plate) ORDER BY issue_date ) AS ViolationNumber
    FROM `jehiah-p1.traffic_violations_311.open_parking_and_camera_violations_nc67_uf89` 
    WHERE VIOLATION = 'PHTO SCHOOL ZN SPEED VIOLATION'
    AND issue_date between '2019-06-01' and '2019-11-01'
)
-- WHERE issue_date between '2019-06-01' and '2019-11-01'
GROUP BY 1, 2
ORDER BY 1

---
Select issue_date, precinct, count(*) as NumberViolations
FROM `jehiah-p1.traffic_violations_311.open_parking_and_camera_violations_nc67_uf89` 
WHERE VIOLATION = 'PHTO SCHOOL ZN SPEED VIOLATION'
AND issue_date between '2019-07-09' and '2019-11-01'
GROUP BY 1, 2
ORDER BY 1, 2




----
-- The Drivers grouped by the number of violations since June
SELECT NumberViolations, count(*) as NumberDrivers
FROM (
    SELECT concat(state, ":", plate) as Vehicle, count(*) as NumberViolations
    FROM `jehiah-p1.traffic_violations_311.nc67_uf89_open_parking_and_camera_violations` 
    WHERE VIOLATION = 'PHTO SCHOOL ZN SPEED VIOLATION'
    AND issue_date between '2019-06-01' and '2019-11-01'
    GROUP BY 1
) 
GROUP BY 1 ORDER BY 1 asc

---
-- Violation Locations
SELECT concat(violation_county, " ", street_name, " ", intersecting_street) as Location, DATE_TRUNC(issue_date, MONTH) as Month,  count(*) as ViolationCount
FROM (
SELECT violation_county, street_name, intersecting_street, issue_date
FROM `jehiah-p1.traffic_violations_311.pvqr_7yc4_parking_violations_issued___fiscal_year_2020` 
WHERE violation_description = "PHTO SCHOOL ZN SPEED VIOLATION"
UNION ALL
SELECT violation_county, street_name, intersecting_street, issue_date
FROM `jehiah-p1.traffic_violations_311.faiq_9dfq_parking_violations_issued___fiscal_year_2019` 
WHERE violation_description = "PHTO SCHOOL ZN SPEED VIOLATION"
AND issue_date >= '2018-09-01'
)
GROUP BY 1, 2
Having ViolationCount > 100
ORDER BY 1, 2
