from kafka import KafkaConsumer
from neo4j import GraphDatabase, exceptions
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


consumer = KafkaConsumer(
    'candidates_topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)   

class Neo4jConnection:
    def __init__(self, uri, user, password):
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            logger.info("Connected to Neo4j successfully.")
        except exceptions.Neo4jError as e:
            logger.error(f"Failed to create the driver: {e}")
            raise e

    def close(self):
        self.driver.close()
        logger.info("Neo4j connection closed.")

    def candidate_exists(self, candidate_id):
        with self.driver.session() as session:
            result = session.execute_read(
                self._check_candidate_exists,
                candidate_id=candidate_id
            )
            return result

    @staticmethod
    def _check_candidate_exists(tx, candidate_id):
        query = "MATCH (c:Candidate {id: $candidate_id}) RETURN COUNT(c) > 0"
        return tx.run(query, candidate_id=candidate_id).single()[0]

    def create_candidate(self, candidate):
        try:
            self.validate_candidate(candidate)
            candidate_id = candidate['id']
            
            if self.candidate_exists(candidate_id):
                logger.info(f"Candidate {candidate_id} already exists. Skipping insertion.")
                return
            
            with self.driver.session() as session:
                session.execute_write(self._create_and_return_candidate, candidate)
                logger.info(f"Candidate {candidate_id} inserted successfully.")
        except (exceptions.Neo4jError, ValueError) as e:
            logger.error(f"Failed to insert candidate {candidate.get('id')}: {e}")

    def validate_candidate(self, candidate):
        if not isinstance(candidate, dict):
            raise ValueError("Candidate data must be a dictionary.")
        
        required_fields = ['id', 'first_name', 'last_name', 'emails', 'phones']
        for field in required_fields:
            if field not in candidate:
                raise ValueError(f"Missing required field: {field}")

        if not isinstance(candidate['emails'], list) or not isinstance(candidate['phones'], list):
            raise ValueError("Emails and phones must be lists.")

    @staticmethod
    def _create_and_return_candidate(tx, candidate):
        try:
            logger.info(f"Creating candidate: {candidate['id']}")
            
            highest_degree = candidate.get('highest_degree_details', {})
            pg_degree = candidate.get('pg_degree', {})
            ug_degree = candidate.get('ug_degree', {})


            if not isinstance(highest_degree, dict):
                highest_degree = {}
            if not isinstance(pg_degree, dict):
                pg_degree = {}
            if not isinstance(ug_degree, dict):
                ug_degree = {}

            full_name = f"{candidate['first_name']} {candidate['last_name']}"
            

            query = (
                "CREATE (c:Candidate {id: $id, first_name: $first_name, last_name: $last_name, full_name: $full_name,"
                " emails: $emails, phones: $phones, skills: $skills, "
                "highest_degree: $highest_degree, pg_degree: $pg_degree, "
                "ug_degree: $ug_degree, ug_graduation_year: $ug_graduation_year, "
                "first_working_date: $first_working_date, state: $state, city: $city, "
                "marital_status: $marital_status, gender: $gender, "
                "skill_bucket: $skill_bucket, domain_experience: $domain_experience}) "
                "RETURN c"
            )
            tx.run(query, 
                id=candidate['id'],
                first_name=candidate['first_name'],
                full_name=full_name, 
                last_name=candidate['last_name'],
                emails=candidate.get('emails', []),
                phones=candidate.get('phones', []),
                skills=candidate.get('skills', []),
                highest_degree=highest_degree.get('degree') if highest_degree else None,
                pg_degree=pg_degree.get('degree') if pg_degree else None,
                ug_degree=ug_degree.get('degree') if ug_degree else None,
                ug_graduation_year=candidate.get('ug_graduation_year'),
                first_working_date=candidate.get('first_working_date'),
                state=candidate.get('state'),
                city=candidate.get('city'),
                marital_status=candidate.get('marital_status'),
                gender=candidate.get('gender'),
                skill_bucket=candidate.get('skill_bucket', []),
                domain_experience=candidate.get('domain_experience', [])
            )

            logger.info(f"Candidate node created for {candidate['id']}")


            ug_graduation_year = candidate.get('ug_graduation_year')
            if ug_graduation_year:
                tx.run(
                    "MERGE (gy:GraduationYear {year: $year}) "
                    "WITH gy "
                    "MATCH (c:Candidate {id: $candidate_id}) "
                    "CREATE (c)-[:GRADUATED_IN]->(gy)",
                    year=ug_graduation_year,
                    candidate_id=candidate['id']
                )
                logger.info(f"GraduationYear node created and relationship established for {candidate['id']}")

            if ug_degree:
                tx.run(
                    "MERGE (col:College {name: $college_name}) "
                    "WITH col "
                    "MATCH (c:Candidate {id: $candidate_id}) "
                    "CREATE (c)-[:HAS_UG_DEGREE {graduation_year: $ug_graduation_year}]->(col)",
                    college_name=ug_degree.get('college', 'Unknown College'),
                    candidate_id=candidate['id'],
                    ug_graduation_year=candidate.get('ug_graduation_year')
                )
                logger.info(f"UG degree relationship created for {candidate['id']}")

            skills = candidate.get('skills', [])
            for skill in skills:
                tx.run(
                    "MERGE (s:Skill {name: $skill_name}) "
                    "WITH s "
                    "MATCH (c:Candidate {id: $candidate_id}) "
                    "CREATE (c)-[:HAS_SKILL]->(s)",
                    skill_name=skill,
                    candidate_id=candidate['id']
                )
                logger.info(f"Skill relationship created for {candidate['id']} with skill {skill}")

            company_history = candidate.get('company_history', [])
            for company in company_history:
                if not isinstance(company, dict):
                    logger.warning(f"Invalid company data: {company}. Skipping.")
                    continue
                tx.run(
                    "MATCH (c:Candidate {id: $id}) "
                    "CREATE (co:Company {company_name: $company_name, start_date: $start_date, "
                    "end_date: $end_date, location: $location, designation: $designation}) "
                    "CREATE (c)-[:WORKED_AT]->(co)",
                    id=candidate['id'],
                    company_name=company.get('company_name', 'Unknown Company'),
                    start_date=company.get('start_date'),
                    end_date=company.get('end_date'), 
                    location=company.get('location', 'Unknown Location'),
                    designation=company.get('designation', 'Unknown Designation')
                )
                logger.info(f"Company relationship created for {candidate['id']} with {company['company_name']}")

                for project in company.get('project_details', []):
                    if isinstance(project, str):
                        tx.run(
                            "MATCH (co:Company {company_name: $company_name}) "
                            "CREATE (p:Project {name: $project}) "
                            "CREATE (co)-[:INVOLVED_IN]->(p)",
                            company_name=company['company_name'],
                            project=project
                        )
                        logger.info(f"Project relationship created for {company['company_name']} with project {project}")
                    else:
                        logger.warning(f"Invalid project data: {project}. Skipping.")

        except exceptions.Neo4jError as e:
            logger.error(f"Error in _create_and_return_candidate: {e}")
            raise e




neo4j_conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "kabil2003")

try:
    for message in consumer:
        candidate = message.value
        logger.info(f"Received candidate: {candidate}")
        neo4j_conn.create_candidate(candidate)
except KeyboardInterrupt:
    logger.info("Process interrupted by user.")
except Exception as e:
    logger.error(f"An unexpected error occurred: {e}")
finally:
    neo4j_conn.close()
