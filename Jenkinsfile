pipeline {
    agent any

    stages {
        stage('Install Docker') {
            steps {
                script {
                    // Install Homebrew if not already installed
                    sh 'which brew || /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'

                    // Install Docker using Homebrew
                    sh 'brew install --cask docker'

                    // Start Docker service
                    sh 'open --background -a Docker'

                    // Wait for Docker to start (adjust the sleep duration if needed)
                    sleep time: 30, unit: 'SECONDS'

                    // Verify Docker installation
                    sh 'docker --version'
                }
            }
        }

        stage('Your Other Stages') {
            // Add your other pipeline stages here
            steps {
                // Example: Build and push a Docker image
                script {
                    sh 'docker build -t your_image_name:latest .'
                    sh 'docker push your_image_name:latest'
                }
            }
        }
    }
}
