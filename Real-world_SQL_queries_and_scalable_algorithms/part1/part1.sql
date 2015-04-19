DROP VIEW IF EXISTS q1a, q1b, q1c, q1d, q2, q3, q4, q5, q6, q7;

-- Question 1a
CREATE VIEW q1a(id, amount)
AS
 select cc.cmte_id AS id , cc.transaction_amt AS amount
 from committee_contributions AS cc 
 where cc.transaction_amt > 5000::numeric;

-- Question 1b
CREATE VIEW q1b(id, name, amount)
AS
  select cc.cmte_id AS id, cc.name AS name, cc.transaction_amt AS amount
  from committee_contributions AS cc
  where cc.transaction_amt > 5000::numeric;

-- Question 1c
CREATE VIEW q1c(id, name, avg_amount)
AS
  select cc.cmte_id AS id, cc.name AS name, AVG(cc.transaction_amt) AS avg_amount
  from committee_contributions AS cc
  where cc.transaction_amt > 5000::numeric
  group by cc.cmte_id, cc.name;

-- Question 1d
CREATE VIEW q1d(id, name, avg_amount)
AS
  select cc.cmte_id AS id, cc.name AS name, AVG(cc.transaction_amt) AS avg_amount
  from committee_contributions AS cc
  where cc.transaction_amt > 5000::numeric
  group by cc.cmte_id, cc.name
  having AVG(cc.transaction_amt) > 10000::numeric;

-- Question 2
CREATE VIEW q2(from_name, to_name)
AS
  select c1.name AS from_name, c2.name AS to_name
  from committees AS c1, committees AS c2, intercommittee_transactions AS ic_t
  where c1.pty_affiliation = 'DEM' and c2.pty_affiliation = 'DEM'
    and ic_t.cmte_id = c2.id and ic_t.other_id = c1.id
  group by c1.id, c2.id
  order by count(*) desc
  limit 10;

-- Question 3
CREATE VIEW q3(name)
AS
  select com.name AS name
  from committees AS com
  where com.id not in (
    select distinct cc.cmte_id
    from committee_contributions AS cc inner join candidates AS ca on cc.cand_id = ca.id
    where ca.name ='OBAMA, BARACK');

-- Question 4.
CREATE VIEW q4 (name)
AS
  select cand.name AS name
  from candidates AS cand inner join committee_contributions AS cc on cc.cand_id = cand.id
  group by cand.id
  having (0.01 * (SELECT COUNT(*) from committees)) <= COUNT(DISTINCT cc.cmte_id);

-- Question 5
CREATE VIEW q5 (name, total_pac_donations)
AS
  select com.name AS name, SUM(ic.transaction_amt)
  from committees AS com
  left outer join individual_contributions AS ic on com.id = ic.cmte_id and ic.entity_tp = 'ORG'
  group by com.id, ic.cmte_id;

-- Question 6
CREATE VIEW q6 (id) AS
  select cc.cand_id AS id
  from committee_contributions AS cc
  where cc.cand_id IS not NULL and entity_tp = 'CCM'
  intersect
  select cc2.cand_id
  from committee_contributions AS cc2
  where cc2.cand_id IS not NULL and entity_tp = 'PAC';

-- Question 7
CREATE VIEW q7 (cand_name1, cand_name2) AS
  select distinct ca1.name AS cand_name1, ca2.name AS cand_name2
  from candidates AS ca1, candidates AS ca2, committee_contributions AS cc1, committee_contributions AS cc2
  where cc1.cand_id = ca1.id and cc1.state = 'RI'
    and cc2.cand_id = ca2.id and cc2.state = 'RI'
    and cc1.cmte_id = cc2.cmte_id and ca1.id != ca2.id;
