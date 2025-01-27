from re import sub
import pg8000
from contentful import Client
from time import sleep
from requests.exceptions import RequestException

# Database connection details

DB_HOST = "xxx"
DB_NAME = "BarTakerDB"
DB_USER = "bartaker_admin"
DB_PASSWORD = "xxxx"

# Contentful connection details
SPACE_ID = "hxu8jsem6qms"
ENVIRONMENT_ID = "master"  # Replace if using a different environment
ACCESS_TOKEN = "xxxx"

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
                RETURNING option_id, question_id, is_correct
            )
            UPDATE v2.user_answers ua
            SET is_correct = uo.is_correct
            FROM updated_option uo
            WHERE ua.chosen_answer_id = uo.option_id
            AND EXISTS (SELECT 1 FROM updated_option)
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
    # Establish connection to the PostgreSQL database using pg8000
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

        # Insert data in the correct order to satisfy foreign key constraints
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
            # Get the subject UUID based on the Contentful ID reference
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
            # Get the topic UUID based on the Contentful ID reference
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
            active_subtopic_ids.add(issue_id)
            # Get the parent subtopic UUID based on the Contentful ID reference
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
        count = 0
        for content_type in ['multipleChoiceQuestion', 'trueFalseQuestion']:
            print(f"Fetching {content_type}...")
            questionType = 'multiple_choice' if content_type == 'multipleChoiceQuestion' else 'true_false'
            
            entries = get_paginated_entries(client, content_type)
            for question in entries:
                count += 1
                question_id = convert_to_uuid(question.sys['id'])
                active_question_ids.add(question_id)
                
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
                    'question_type': questionType
                })

                # Step 5: Insert Options for each question
                if questionType == 'multiple_choice':
                    answer_options = question.raw['fields']['answerOptions']
                    for answer in answer_options:
                        answer_id = answer['sys']['id']
                        entity = client.entries({'sys.id[in]': answer_id})
                        option_id = convert_to_uuid(answer_id)
                        active_option_ids.add(option_id)
                        entity = entity[0]
                        insert_data(cursor, 'options', {
                            'option_id': option_id,
                            'question_id': question_id,
                            'option_text': entity.raw['fields']['answerText'],
                            'is_correct': entity.raw['fields']['isCorrectAnswer']
                        })
                elif questionType == 'true_false':
                    for option in ['True', 'False']:
                        # Create a deterministic ID by combining question ID and T/F value
                        option_id = convert_to_uuid(f"{question.sys['id']}_{option}")
                        active_option_ids.add(option_id)
                        isCorrectAnswer = str(question.raw['fields']['correctAnswer']).lower() == option.lower()
                        insert_data(cursor, 'options', {
                            'option_id': option_id,
                            'question_id': question_id,
                            'option_text': option,
                            'is_correct': isCorrectAnswer
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

        print(f"Total number of questions processed: {len(active_question_ids)}")

        # Step 6: Insert Quizzes
        quiz_entries = client.entries({'content_type': '4W0to1SsFsewSPWUfFJzGC'})  # Replace with your Contentful quiz content type ID
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
                        insert_data(cursor, 'quiz_questions', {
                            'quiz_id': quiz_id,
                            'question_id': question_id,
                            'question_order': order + 1
                        })
                    else:
                        print(f"Warning: Question {question_id} not found in questions table. Skipping.")

        print("Contentful data successfully synchronized with the database!")
        print(count)

        # Clean up stale data in the correct order to respect foreign key constraints
        print("\nCleaning up stale data...")
        # First delete quiz_questions (no dependencies)
        delete_stale_data(cursor, 'quiz_questions', active_quiz_question_pairs)
        
        # Delete quiz entries (no dependencies)
        delete_stale_data(cursor, 'quiz', active_quiz_ids)
        
        # Delete user_answers that reference stale options
        cursor.execute("""
            DELETE FROM v2.user_answers
            WHERE chosen_answer_id NOT IN (
                SELECT unnest(%s::uuid[])
            )
        """, (list(active_option_ids),))
        print(f"Deleted {cursor.rowcount} stale user answers")
        
        # Now we can safely delete options
        delete_stale_data(cursor, 'options', active_option_ids)
        
        # Delete questions (will cascade to user_answers)
        delete_stale_data(cursor, 'questions', active_question_ids)
        
        # Delete subtopics (will cascade to questions)
        delete_stale_data(cursor, 'subtopics', active_subtopic_ids)
        
        # Delete topics (will cascade to subtopics)
        delete_stale_data(cursor, 'topics', active_topic_ids)
        
        # Delete subscriptions that reference stale subjects
        cursor.execute("""
            DELETE FROM v2.subscriptions
            WHERE subject_id NOT IN (
                SELECT unnest(%s::uuid[])
            )
        """, (list(active_subject_ids),))
        print(f"Deleted {cursor.rowcount} stale subscriptions")
        
        # Finally delete subjects
        delete_stale_data(cursor, 'subjects', active_subject_ids)

        # Close the cursor and connection after data insertion
        cursor.close()
        conn.close()

        print("\n=== Data Migration Analytics ===\n")
        
        # Reopen connection for analytics
        conn = pg8000.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Analytics checks continue here...
        # Check subjects
        cursor.execute("""
            SELECT subject_name, subject_jurisdiction 
            FROM v2.subjects 
            ORDER BY subject_name
        """)
        subjects = cursor.fetchall()
        print(f"Total Subjects: {len(subjects)}")
        print("Subjects List:")
        for subject in subjects:
            print(f"- {subject[0]} ({subject[1]})")

        # Check questions and their answer choices
        cursor.execute("""
            SELECT 
                q.question_type,
                COUNT(*) as total_questions,
                COUNT(CASE WHEN q.correct_answer_id IS NULL THEN 1 END) as missing_correct_answer
            FROM v2.questions q
            GROUP BY q.question_type
        """)
        question_stats = cursor.fetchall()
        print("\nQuestion Statistics:")
        for stat in question_stats:
            print(f"- {stat[0]}: {stat[1]} questions (Missing correct answer: {stat[2]})")

        # Check questions without any options
        cursor.execute("""
            SELECT COUNT(*) 
            FROM v2.questions q
            LEFT JOIN v2.options o ON q.question_id = o.question_id
            WHERE o.option_id IS NULL
        """)
        questions_without_options = cursor.fetchone()[0]
        print(f"Questions without any options: {questions_without_options}")

        # Check quizzes and their questions
        cursor.execute("""
            SELECT 
                q.quiz_name,
                q.distinction,
                COUNT(qq.question_id) as question_count
            FROM v2.quiz q
            LEFT JOIN v2.quiz_questions qq ON q.quiz_id = qq.quiz_id
            GROUP BY q.quiz_id, q.quiz_name, q.distinction
            ORDER BY q.quiz_name
        """)
        quiz_stats = cursor.fetchall()
        print("\nQuiz Statistics:")
        print(f"Total Quizzes: {len(quiz_stats)}")
        print("Quiz Details:")
        for quiz in quiz_stats:
            print(f"- {quiz[0]} ({quiz[1] or 'No distinction'}): {quiz[2]} questions")

        # Add to analytics section
        cursor.execute("""
            SELECT 
                COUNT(*) as total_answers,
                COUNT(CASE WHEN ua.is_correct != (ua.chosen_answer_id = q.correct_answer_id) THEN 1 END) as mismatched_correctness
            FROM v2.user_answers ua
            JOIN v2.questions q ON ua.question_id = q.question_id
        """)
        integrity_check = cursor.fetchone()
        print(f"\nUser Answer Integrity Check:")
        print(f"Total answers: {integrity_check[0]}")
        print(f"Answers with mismatched correctness: {integrity_check[1]}")

        print("\n=== End of Analytics ===\n")

    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        cursor.close()
        conn.close()


# Run the function to insert data
if __name__ == "__main__":
    insert_contentful_data()