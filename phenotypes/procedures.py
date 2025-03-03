from google.cloud import bigquery
import pandas as pd

CURRENT_DATASET = 'fc-aou-cdr-prod.R2024Q3R5'
query=f""" 
    SELECT 
        SPLIT(question, ': ')[OFFSET(0)] as topic
        , SPLIT(question, ': ')[OFFSET(1)] as question
        , question_concept_id
        , COUNT(DISTINCT person_id) as N_participants
        
    FROM `{CURRENT_DATASET}.ds_survey`
    WHERE survey = 'The Basics' 
    GROUP BY 1, 2, 3
    ORDER BY topic, question_concept_id  
    """ 
    
basics_all_questions_df = pd.read_gbq(query)
basics_all_questions_df.set_index(['topic', 'question', 'question_concept_id'])

def basics(concept_id):

    query=f"""
              SELECT answer as Answer, N_participants, round(100*(N_participants / total),2) AS Percentage
              FROM (SELECT answer, COUNT(DISTINCT person_id) AS N_participants, SUM(COUNT(DISTINCT person_id)) OVER() AS total
                    FROM `{CURRENT_DATASET}.ds_survey`
                    WHERE question_concept_id = {concept_id}
                    GROUP BY answer)
              ORDER BY N_participants DESC"""

    df = pd.read_gbq(query,dialect = "standard")
    df.loc[len(df.index)] = ['TOTAL', df.N_participants.sum(), round(df.Percentage.sum())]
    df['Answer'].replace('PMI: ','',regex=True, inplace=True)

    return(df)


# Sex at birth
basics(concept_id=1585845)
