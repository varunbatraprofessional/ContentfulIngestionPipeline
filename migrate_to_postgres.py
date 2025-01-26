from re import sub
import pg8000
from contentful import Client
from time import sleep
from requests.exceptions import RequestException

# Database connection details

DB_HOST = "bartaker-encrypted-2.cvmyym82q7cm.us-east-2.rds.amazonaws.com"
DB_NAME = "BarTakerDB"
DB_USER = "postgres"
DB_PASSWORD = "xxxxxx"

# Contentful connection details
SPACE_ID = "hxu8jsem6qms"
ENVIRONMENT_ID = "master"  # Replace if using a different environment
ACCESS_TOKEN = "xxxxxx"

# Create a Contentful client instance with longer timeout
client = Client(
    SPACE_ID, 
    ACCESS_TOKEN, 
    environment=ENVIRONMENT_ID,
    timeout_s=30
)

import uuid

# Define a namespace (it can be any UUID, but it must be consistent)
NAMESPACE_UUID = uuid.UUID('12345678-1234-5678-1234-567812345678')

def convert_to_uuid(contentful_id):
    # Generate a UUID based on the Contentful ID and namespace
    return str(uuid.uuid5(NAMESPACE_UUID, contentful_id))


# Helper function to get a UUID by querying the ID field in the database
def get_uuid_by_id(cursor, table_name, id_value):
    cursor.execute(
        f"SELECT {table_name[:-1]}_id FROM v2.{table_name} WHERE {table_name[:-1]}_id = %s",
        (id_value,)
    )
    result = cursor.fetchone()
    return result[0] if result else None


# Helper function to insert data with a specific set of fields
def insert_data(cursor, table_name, data_dict):
    columns = ', '.join(data_dict.keys())
    values_placeholder = ', '.join(['%s'] * len(data_dict))
    
    if table_name == 'quiz_questions':
        query = f"""
            INSERT INTO v2.{table_name} ({columns}) 
            VALUES ({values_placeholder})
            ON CONFLICT (quiz_id, question_id) DO UPDATE 
            SET question_order = EXCLUDED.question_order
        """
    elif table_name == 'options':
        query = f"""
            WITH updated_option AS (
                INSERT INTO v2.{table_name} ({columns}) 
                VALUES ({values_placeholder})
                ON CONFLICT (option_id) DO UPDATE 
                SET 
                    option_text = EXCLUDED.option_text,
                    is_correct = EXCLUDED.is_correct
                WHERE 
                    v2.{table_name}.option_id = EXCLUDED.option_id AND
                    (v2.{table_name}.option_text != EXCLUDED.option_text OR
                     v2.{table_name}.is_correct != EXCLUDED.is_correct)
                RETURNING option_id
            )
            SELECT * FROM updated_option
        """
    elif table_name == 'questions':
        query = f"""
            WITH updated_question AS (
                INSERT INTO v2.{table_name} ({columns}) 
                VALUES ({values_placeholder})
                ON CONFLICT (question_id) DO UPDATE 
                SET 
                    question_text = EXCLUDED.question_text,
                    correct_answer_id = EXCLUDED.correct_answer_id,
                    explanation = EXCLUDED.explanation,
                    subtopic_id = EXCLUDED.subtopic_id,
                    question_type = EXCLUDED.question_type
                WHERE 
                    v2.{table_name}.question_id = EXCLUDED.question_id AND
                    (v2.{table_name}.question_text != EXCLUDED.question_text OR
                     v2.{table_name}.correct_answer_id IS DISTINCT FROM EXCLUDED.correct_answer_id OR
                     v2.{table_name}.explanation IS DISTINCT FROM EXCLUDED.explanation OR
                     v2.{table_name}.subtopic_id IS DISTINCT FROM EXCLUDED.subtopic_id OR
                     v2.{table_name}.question_type != EXCLUDED.question_type)
                RETURNING question_id, correct_answer_id
            )
            UPDATE v2.user_answers ua
            SET is_correct = (ua.chosen_answer_id = uq.correct_answer_id)
            FROM updated_question uq
            WHERE ua.question_id = uq.question_id
            AND ua.chosen_answer_id IS NOT NULL
            AND uq.correct_answer_id IS NOT NULL
            AND EXISTS (SELECT 1 FROM updated_question)
        """
    else:
        # Original logic for other tables
        update_set = ', '.join([f"{k} = EXCLUDED.{k}" for k in data_dict.keys()])
        primary_key = f"{table_name[:-1] if table_name != 'quiz' else 'quiz'}_id"
        query = f"""
            INSERT INTO v2.{table_name} ({columns}) 
            VALUES ({values_placeholder})
            ON CONFLICT ({primary_key}) DO UPDATE 
            SET {update_set}
        """
    
    cursor.execute(query, tuple(data_dict.values()))


def get_contentful_entries_with_retry(client, query, max_retries=3, delay=5):
    """
    Fetch entries from Contentful with retry logic
    """
    for attempt in range(max_retries):
        try:
            return client.entries(query)
        except Exception as e:
            if attempt == max_retries - 1:  # Last attempt
                raise e
            print(f"Attempt {attempt + 1} failed. Retrying in {delay} seconds...")
            sleep(delay)
            delay *= 2  # Exponential backoff

def get_paginated_entries(client, content_type, batch_size=100):
    """
    Get all entries of a content type using pagination
    """
    skip = 0
    all_entries = []
    
    while True:
        try:
            entries = get_contentful_entries_with_retry(client, {
                'content_type': content_type,
                'limit': batch_size,
                'skip': skip
            })
            
            if not entries:
                break
                
            all_entries.extend(entries)
            print(f"Retrieved {len(all_entries)} {content_type} entries so far...")
            
            if len(entries) < batch_size:
                break
                
            skip += batch_size
            
        except Exception as e:
            print(f"Error retrieving {content_type} entries at skip={skip}: {e}")
            raise
            
    return all_entries

def delete_stale_data(cursor, table_name, active_ids):
    """
    Delete records that exist in the database but not in Contentful
    Args:
        cursor: Database cursor
        table_name: Name of the table to clean up
        active_ids: Set of UUIDs that are currently active in Contentful
    """
    id_column = f"{table_name[:-1] if table_name != 'quiz' else 'quiz'}_id"
    
    # Special handling for quiz_questions table
    if table_name == 'quiz_questions':
        cursor.execute(f"""
            DELETE FROM v2.{table_name}
            WHERE (quiz_id, question_id) NOT IN (
                SELECT quiz_id, question_id 
                FROM unnest(%s::uuid[], %s::uuid[]) AS t(quiz_id, question_id)
            )
        """, ([x[0] for x in active_ids], [x[1] for x in active_ids]))
    else:
        cursor.execute(f"""
            DELETE FROM v2.{table_name}
            WHERE {id_column} NOT IN (
                SELECT unnest(%s::uuid[])
            )
        """, (list(active_ids),))
    
    deleted_count = cursor.rowcount
    print(f"Deleted {deleted_count} stale records from {table_name}")
    return deleted_count

# Insert Contentful data into the PostgreSQL schema with proper foreign key handling
def insert_contentful_data():
    print("Starting to insert data")
    conn = pg8000.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        # Track active IDs for each table
        active_subject_ids = set()
        active_topic_ids = set()
        active_subtopic_ids = set()
        active_question_ids = set()
        active_option_ids = set()
        active_quiz_ids = set()
        active_quiz_question_pairs = set()  # Will store (quiz_id, question_id) tuples

        # Step 1: Insert Subjects with retry logic
        print("Fetching subjects...")
        subject_entries = get_paginated_entries(client, '2UVKc9N9FTQ9lfqyfwQaGl')
        for subject in subject_entries:
            subject_id = convert_to_uuid(subject.sys['id'])
            active_subject_ids.add(subject_id)
            is_free = False
            if subject.name == 'Evidence':
                is_free = True
            insert_data(cursor, 'subjects', {'subject_id': subject_id, 'subject_name': subject.name, 'subject_jurisdiction': subject.jurisdiction, 'is_free': is_free})

        # Step 2: Insert Topics with retry logic
        print("Fetching topics...")
        topic_entries = get_paginated_entries(client, '60H8p8k0YxbzjCVXs30xEA')
        for topic in topic_entries:
            topic_id = convert_to_uuid(topic.sys['id'])
            active_topic_ids.add(topic_id)
            subject_id = get_uuid_by_id(cursor, "subjects", convert_to_uuid(topic.raw['fields']['subjectReference']['sys']['id']))
            if subject_id:
                insert_data(cursor, 'topics', {
                    'topic_id': topic_id,
                    'topic_name': topic.name,
                    'subject_id': subject_id
                })

        # Step 3: Insert Subtopics and Issues with retry logic
        print("Fetching subtopics...")
        subtopic_entries = get_paginated_entries(client, '4ISm6Gy7vvKHsaIhOybTmh')
        print("Fetching issues...")
        issue_entries = get_paginated_entries(client, '71Bp6hF5Z1rB75OvLZH5Mk')

        # Insert Subtopics
        for subtopic in subtopic_entries:
            subtopic_id = convert_to_uuid(subtopic.sys['id'])
            active_subtopic_ids.add(subtopic_id)
            topic_id = get_uuid_by_id(cursor, "topics", convert_to_uuid(subtopic.raw['fields']['topicReference']['sys']['id']))
            if topic_id:
                insert_data(cursor, 'subtopics', {
                    'subtopic_id': subtopic_id,
                    'subtopic_name': subtopic.name,
                    'topic_id': topic_id,  # Assign the topic_id directly from the reference
                    'parent_subtopic_id': None  # Subtopics do not have parent subtopics
                })

        # Insert Issues as children of Subtopics
        for issue in issue_entries:
            issue_id = convert_to_uuid(issue.sys['id'])
            active_subtopic_ids.add(issue_id)  # Issues are stored in subtopics table
            parent_subtopic_id = get_uuid_by_id(cursor, "subtopics", convert_to_uuid(issue.raw['fields']['subtopicReference']['sys']['id']))
            if parent_subtopic_id:
                # Retrieve the topic_id of the parent subtopic
                cursor.execute(
                    "SELECT topic_id FROM v2.subtopics WHERE subtopic_id = %s",
                    (parent_subtopic_id,)
                )
                topic_id = cursor.fetchone()[0] if cursor.rowcount > 0 else None
                insert_data(cursor, 'subtopics', {
                    'subtopic_id': issue_id,  # Use subtopic_id since we're treating issues as subtopics
                    'subtopic_name': issue.name,
                    'topic_id': topic_id,  # Assign the same topic_id as the parent subtopic
                    'parent_subtopic_id': parent_subtopic_id  # Issues are children of a subtopic
                })

        # Step 4: Process Questions
        for content_type in ['multipleChoiceQuestion', 'trueFalseQuestion']:
            entries = get_paginated_entries(client, content_type)
            for question in entries:
                question_id = convert_to_uuid(question.sys['id'])
                active_question_ids.add(question_id)
                
                # Track option IDs
                if content_type == 'multipleChoiceQuestion':
                    for answer in question.raw['fields']['answerOptions']:
                        option_id = convert_to_uuid(answer['sys']['id'])
                        active_option_ids.add(option_id)
                else:  # true_false
                    for option in ['True', 'False']:
                        option_id = convert_to_uuid(f"{question.sys['id']}_{option}")
                        active_option_ids.add(option_id)

                # Process question (existing question processing logic here)
                question_text = question.raw['fields']['questionText']
                hierarchy_ref = convert_to_uuid(question.raw['fields']['hierarchyReference']['sys']['id'])
                hierarchy_level = question.raw['fields']['contentHierarchyLevelText']

                # Initialize the IDs to None
                subject_id = None
                topic_id = None
                subtopic_id = None

                # Determine the parent entity type and get the appropriate ID
                if hierarchy_level == "Subject":
                    subject_id = get_uuid_by_id(cursor, "subjects", hierarchy_ref)
                elif hierarchy_level == "Topic":
                    topic_id = get_uuid_by_id(cursor, "topics", hierarchy_ref)
                    cursor.execute("SELECT subject_id FROM v2.topics WHERE topic_id = %s", (topic_id,))
                    subject_id = cursor.fetchone()[0] if cursor.rowcount > 0 else None
                elif hierarchy_level == "Subtopic":
                    subtopic_id = get_uuid_by_id(cursor, "subtopics", hierarchy_ref)
                    if subtopic_id:
                        # Retrieve the topic_id of the parent subtopic
                        cursor.execute("SELECT topic_id FROM v2.subtopics WHERE subtopic_id = %s", (subtopic_id,))
                        topic_id = cursor.fetchone()[0] if cursor.rowcount > 0 else None
                        cursor.execute("SELECT subject_id FROM v2.topics WHERE topic_id = %s", (topic_id,))
                        subject_id = cursor.fetchone()[0] if cursor.rowcount > 0 else None

                elif hierarchy_level == "Issue":
                    subtopic_id = get_uuid_by_id(cursor, "subtopics", hierarchy_ref)
                    # Issues are already in subtopics table; get the topic_id of parent issue
                    if subtopic_id:
                        # Retrieve the topic_id of the parent subtopic (issue)
                        cursor.execute("SELECT topic_id FROM v2.subtopics WHERE subtopic_id = %s", (subtopic_id,))
                        topic_id = cursor.fetchone()[0] if cursor.rowcount > 0 else None
                        cursor.execute("SELECT subject_id FROM v2.topics WHERE topic_id = %s", (topic_id,))
                        subject_id = cursor.fetchone()[0] if cursor.rowcount > 0 else None

                # Insert the question with appropriate references
                insert_data(cursor, 'questions', {
                    'question_id': question_id,
                    'question_text': question_text,
                    'subtopic_id': subtopic_id,
                    'subject_id': subject_id,
                    'topic_id': topic_id,
                    'question_type': 'multiple_choice' if content_type == 'multipleChoiceQuestion' else 'true_false'
                })

                # Move these outside the if/elif block since they apply to both types
                cursor.execute(
                    "SELECT option_id FROM v2.options WHERE question_id = %s AND is_correct = %s",
                    (question_id, True)
                )
                correct_option_id = cursor.fetchone()[0] if cursor.rowcount > 0 else None
                cursor.execute(
                    "UPDATE v2.questions SET correct_answer_id = %s WHERE question_id = %s",
                    (correct_option_id, question_id)
                )
                if 'answerExplanation' in question.raw['fields']:
                    cursor.execute(
                        "UPDATE v2.questions SET explanation = %s WHERE question_id = %s",
                        (question.raw['fields']['answerExplanation'], question_id)
                    )

        # Step 6: Insert Quizzes
        quiz_entries = client.entries({'content_type': '4W0to1SsFsewSPWUfFJzGC'})
        for quiz in quiz_entries:
            quiz_id = convert_to_uuid(quiz.sys['id'])
            active_quiz_ids.add(quiz_id)
            quiz_name = quiz.raw['fields']['name']
            
            # Optional links to subject, topic, and subtopic based on hierarchy level
            subject_id = None
            topic_id = None
            subtopic_id = None
            
            # Determine if a subject, topic, or subtopic reference exists
            if 'subjectReference' in quiz.raw['fields']:
                subject_id = get_uuid_by_id(cursor, "subjects", convert_to_uuid(quiz.raw['fields']['subjectReference']['sys']['id']))
            if 'topicReference' in quiz.raw['fields']:
                topic_id = get_uuid_by_id(cursor, "topics", convert_to_uuid(quiz.raw['fields']['topicReference']['sys']['id']))
            if 'subtopicReference' in quiz.raw['fields']:
                subtopic_id = get_uuid_by_id(cursor, "subtopics", convert_to_uuid(quiz.raw['fields']['subtopicReference']['sys']['id']))

            # Insert the quiz record
            insert_data(cursor, 'quiz', {
                'quiz_id': quiz_id,
                'quiz_name': quiz_name,
                'subject_id': subject_id,
                'topic_id': topic_id,
                'subtopic_id': subtopic_id,
                'distinction': quiz.raw['fields'].get('distinction', None)  # Optional distinction field
            })

            # Step 7: Insert Quiz Questions (with validation)
            if 'questions' in quiz.raw['fields']:
                questions = quiz.raw['fields']['questions']
                for order, question_ref in enumerate(questions):
                    question_id = convert_to_uuid(question_ref['sys']['id'])
                    if question_id in active_question_ids:
                        active_quiz_question_pairs.add((quiz_id, question_id))
                    else:
                        print(f"Warning: Question {question_id} not found in questions table. Skipping.")

        # Clean up stale data in the correct order to respect foreign key constraints
        print("\nCleaning up stale data...")
        # First delete quiz_questions (no dependencies)
        delete_stale_data(cursor, 'quiz_questions', active_quiz_question_pairs)
        
        # Delete quiz entries (no dependencies)
        delete_stale_data(cursor, 'quiz', active_quiz_ids)
        
        # Delete options before questions due to correct_answer_id reference
        delete_stale_data(cursor, 'options', active_option_ids)
        
        # Delete questions (will cascade to user_answers)
        delete_stale_data(cursor, 'questions', active_question_ids)
        
        # Delete subtopics (will cascade to questions)
        delete_stale_data(cursor, 'subtopics', active_subtopic_ids)
        
        # Delete topics (will cascade to subtopics)
        delete_stale_data(cursor, 'topics', active_topic_ids)
        
        # Finally delete subjects (will cascade to topics and subscriptions)
        delete_stale_data(cursor, 'subjects', active_subject_ids)

        print("Contentful data successfully synchronized with the database!")

    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        cursor.close()
        conn.close()


# Run the function to insert data
if __name__ == "__main__":
    insert_contentful_data()
