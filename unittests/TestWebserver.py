import unittest
import pandas
from app.task_runner import TaskRunner

class CustomDataIngestor:
    """Data Ingestor reads from CSV and stores server the data that will be queried"""

    def __init__(self, dataframe):
        #  Read csv from csv_path
        self.csv_file = dataframe

        self.questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        self.questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week',
            'Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
        ]

class TestWebserver(unittest.TestCase):
    def setUp(self):
        # Build Pandas Dataframe
        self.question = 'Percent of adults aged 18 years and older who have obesity'
        data = {
            'LocationDesc': ['Ohio', 'New Mexico', 'Tennessee', 'Ohio'],
            'Question': [self.question, self.question, self.question, self.question],
            'Data_Value': [29.4, 27.7, 44.2, 31.6],
            'Stratification1': ['35 - 44', '45 - 54', '2 or more races', '$15,000 - $24,999'],
            'StratificationCategory1': ['Age (years)', 'Age (years)', 'Race/Ethnicity', 'Income']
        }
        self.dataframe = pandas.DataFrame(data)

        # Build data ingestor
        self.data_ingestor = CustomDataIngestor(self.dataframe)

    def test_states_mean(self):
        # Create args for function
        task = {}
        task['question'] = self.question
        task['task'] = 'states_mean'
        task['job_id'] = 1

        # Run states_mean
        task_runner = TaskRunner(1, None)
        result = task_runner.states_mean(task, write = False, csv_file = self.dataframe)

        expected = {
            'New Mexico': 27.7,
            'Ohio': 30.5,
            'Tennessee': 44.2
        }

        self.assertEqual(result, expected)
    
    def test_state_mean(self):
        # Create args for function
        task = {}
        task['question'] = self.question
        task['state'] = 'Ohio'
        task['task'] = 'state_mean'
        task['job_id'] = 1

        # Run state_mean
        task_runner = TaskRunner(1, None)
        result = task_runner.state_mean(task, write = False, csv_file = self.dataframe)

        expected = {
            'Ohio': 30.5
        }

        self.assertEqual(result, expected)

    def test_best5(self):
        # Create args for function
        task = {}
        task['question'] = self.question
        task['task'] = 'best5'
        task['job_id'] = 1

        # Run best5
        task_runner = TaskRunner(1, None)
        result = task_runner.best5(task, write = False, data_ingestor = self.data_ingestor)

        expected = {
            'New Mexico': 27.7,
            'Ohio': 30.5,
            'Tennessee': 44.2
        }

        self.assertEqual(result, expected)
    
    def test_worst5(self):
        # Create args for function
        task = {}
        task['question'] = self.question
        task['task'] = 'worst5'
        task['job_id'] = 1

        # Run best5
        task_runner = TaskRunner(1, None)
        result = task_runner.worst5(task, write = False, data_ingestor = self.data_ingestor)

        expected = {
            'Tennessee': 44.2,
            'Ohio': 30.5,
            'New Mexico': 27.7
        }

        self.assertEqual(result, expected)

    def test_global_mean(self):
        # Create args for function
        task = {}
        task['question'] = self.question
        task['task'] = 'global_mean'
        task['job_id'] = 1

        # Run global_mean
        task_runner = TaskRunner(1, None)
        result = task_runner.global_mean(task, write = False, csv_file = self.dataframe)

        expected = {
            'global_mean': 33.225
        }

        self.assertEqual(result, expected)
    
    def test_diff_from_mean(self):
        # Create args for function
        task = {}
        task['question'] = self.question
        task['task'] = 'diff_from_mean'
        task['job_id'] = 1

        # Run diff_from_mean
        task_runner = TaskRunner(1, None)
        result = task_runner.diff_from_mean(task, write = False, csv_file = self.dataframe)

        expected = {
            'New Mexico': 5.525,
            'Ohio': 2.725,
            'Tennessee': -10.975
        }

        for key in expected:
            self.assertEqual(round(result[key], 5), expected[key])

    def test_state_diff_from_mean(self):
        # Create args for function
        task = {}
        task['question'] = self.question
        task['task'] = 'state_diff_from_mean'
        task['state'] = 'Ohio'
        task['job_id'] = 1

        # Run state_diff_from_mean
        task_runner = TaskRunner(1, None)
        result = task_runner.state_diff_from_mean(task, write = False, csv_file = self.dataframe)

        expected = {
            'Ohio': 2.725
        }

        self.assertEqual(round(result['Ohio'], 5), expected['Ohio'])
    
    def test_mean_by_category(self):
        # Create args for function
        task = {}
        task['question'] = self.question
        task['task'] = 'mean_by_category'
        task['job_id'] = 1

        # Run state_diff_from_mean
        task_runner = TaskRunner(1, None)
        result = task_runner.mean_by_category(task, write = False, csv_file = self.dataframe)

        expected = {
            ('New Mexico', 'Age (years)', '45 - 54'): 27.7,
            ('Ohio', 'Age (years)', '35 - 44'): 29.4,
            ('Ohio', 'Income', '$15,000 - $24,999'): 31.6,
            ('Tennessee', 'Race/Ethnicity', '2 or more races'): 44.2
        }

        self.assertEqual(result, expected)
    
    def test_state_mean_by_category(self):
        # Create args for function
        task = {}
        task['question'] = self.question
        task['task'] = 'state_mean_by_category'
        task['state'] = 'Ohio'
        task['job_id'] = 1

        # Run state_diff_from_mean
        task_runner = TaskRunner(1, None)
        result = task_runner.state_mean_by_category(task, write = False, csv_file = self.dataframe)

        expected = {
            ('Age (years)', '35 - 44'): 29.4,
            ('Income', '$15,000 - $24,999'): 31.6
        }

        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()