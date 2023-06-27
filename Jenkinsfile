pipeline {
  agent any

  parameters {
        string(defaultValue: '', description: 'Start Date (YYYY-MM-DD)', name: 'START_DATE')
        string(defaultValue: '', description: 'End Date (YYYY-MM-DD)', name: 'END_DATE')
    }

  environment {
    CONSUMER_KEY = credentials('consumer-key') // Use Jenkins credentials to securely store the consumer key
    CONSUMER_SECRET = credentials('consumer-secret') // Use Jenkins credentials to securely store the consumer secret
  }

  stages {
    stage('Checkout') {
            steps {
                // Checkout the repository
                checkout scm
            }
        }

      stage('Install Dependencies') {
            steps {
              script {
                // Install Poetry
                sh 'pip install poetry'

                // Activate Poetry virtual env and install project dependencies
                sh 'poetry install'

              }
            }
        }

      stage('Data Loading') {
            steps {
                script {
                    // Execute the Python script to load data
                    def start_date = params.START_DATE
                    def end_date = params.END_DATE

                    // Retrieve hex data
                    def hex_data = sh(script: """
                        poetry run python -c "
                        import os
                        from data_loader import DataLoader, DataLoaderException

                        # Set environment variables
                        os.environ['CONSUMER_KEY'] = '${CONSUMER_KEY}'
                        os.environ['CONSUMER_SECRET'] = '${CONSUMER_SECRET}'

                        # API endpoint for hex data
                        api_endpoint = 'https://api.business.bt.com/v1/footfall/reports/hex-grid/tfl?agg=time_indicator'

                        # Create an instance of DataLoader
                        data_loader = DataLoader(api_endpoint)

                        # Retrieve hex data
                        hex_data = data_loader.get_hex_data('${start_date}', '${end_date}')

                        # Print the hex data
                        print(hex_data)
                        "
                    """, returnStdout: true).trim()

                    // Pass hex data as an environment variable to the next stage
                    env.HEX_DATA = hex_data
                }
            }
        }

    stage('fetch Data') {
      steps {
        dir('highstreets/data_source_sink') {
          script {
            sh 'export CONSUMER_KEY=$CONSUMER_KEY'
            sh 'export CONSUMER_SECRET=$CONSUMER_SECRET'

            // Run the python script to load data
            withEnv(["PATH+POETRY=${env.WORKSPACE}/.poetry/bin"]) {
              sh 'python dataloader.py ${START_DATE} ${END_DATE}'
            }
          }
        }
      }
    }

    stage('Transform Data') {
      steps {
        dir('highstreets/data_transformation') {
          sh 'export CONSUMER_KEY=$CONSUMER_KEY'
          sh 'export CONSUMER_SECRET=$CONSUMER_SECRET'
          sh 'python hextransform.py'
        }
      }
    }
  }
}
