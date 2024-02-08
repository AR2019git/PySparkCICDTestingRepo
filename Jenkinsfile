pipeline {
  agent {dockerfile {
  args "-u jenkins"}
  }
  stages {
        stage('Install Python') {
            steps {
                script {
                    // Install Python 3.7 using a package manager (e.g., apt-get)
                    sh 'sudo apt-get update && sudo apt-get install -y python3.7'
                }
            }
        }
    stage("prepare") {
      steps {
        script{
        sh "pipenv install --dev"
        }
      }
    }
    stage("test"){
      steps{
        sh "pipenv run pytest"
      }
    }
    stage("prepare artifact"){
      steps{
        sh "make build"
      }
    }
    stage("publish artifact"){
      steps{
        sh "aws s3 cp packages.zip s3://some-s3-path/"
      }
    }
  }
}
