pipeline {
    agent any
    
    environment {
        // Dev credentials
        DEV_DB_HOST = credentials('DEV_DB_HOST')
        DEV_DB_PASSWORD = credentials('DEV_DB_PASSWORD')
        
        // Prod credentials
        PROD_DB_HOST = credentials('PROD_DB_HOST')
        PROD_DB_PASSWORD = credentials('PROD_DB_PASSWORD')
        
        // Contentful credentials
        CONTENTFUL_ACCESS_TOKEN = credentials('CONTENTFUL_ACCESS_TOKEN')
        
        // Python virtual environment path
        VENV_PATH = "${WORKSPACE}/venv"
    }
    
    stages {
        stage('Setup Python Environment') {
            steps {
                script {
                    // Create and activate virtual environment
                    sh '''
                        python3 -m venv ${VENV_PATH}
                        . ${VENV_PATH}/bin/activate
                        pip install pg8000 contentful requests
                    '''
                }
            }
        }
        
        stage('Dev Migration') {
            steps {
                script {
                    echo 'Starting migration to Dev environment...'
                    withEnv([
                        "DB_HOST=${DEV_DB_HOST}",
                        "DB_PASSWORD=${DEV_DB_PASSWORD}",
                        "ACCESS_TOKEN=${CONTENTFUL_ACCESS_TOKEN}"
                    ]) {
                        sh '''
                            . ${VENV_PATH}/bin/activate
                            python migrate_to_postgres.py
                        '''
                    }
                }
            }
        }
        
        stage('Approval for Production') {
            steps {
                script {
                    // Add timestamp for reference
                    def timestamp = new Date().format("yyyy-MM-dd HH:mm:ss")
                    input message: """
                        Dev migration completed successfully at ${timestamp}.
                        Please verify the data in Dev environment before proceeding to Production.
                        
                        Do you want to proceed with the Production migration?
                    """
                }
            }
        }
        
        stage('Prod Migration') {
            steps {
                script {
                    echo 'Starting migration to Production environment...'
                    withEnv([
                        "DB_HOST=${PROD_DB_HOST}",
                        "DB_PASSWORD=${PROD_DB_PASSWORD}",
                        "ACCESS_TOKEN=${CONTENTFUL_ACCESS_TOKEN}"
                    ]) {
                        sh '''
                            . ${VENV_PATH}/bin/activate
                            python migrate_to_postgres.py
                        '''
                    }
                }
            }
        }
    }
    
    post {
        always {
            // Clean up
            cleanWs()
        }
        success {
            echo 'Migration completed successfully in both environments!'
        }
        failure {
            echo 'Migration failed!'
            // You can add notification steps here (email, Slack, etc.)
        }
    }
} 