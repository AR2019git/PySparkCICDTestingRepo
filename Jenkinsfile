pipeline {
  agent {dockerfile {
  args "-u jenkins"}
  }
  stages {
  
    stage("prepare") {
      steps {
        script{
        sh "pipenv install --dev"
        }
      }
    }
    stage("test"){
      steps{
        sh "pipenv run pytest > testlogs.txt"
        sh "more testlogs.txt"
        sh "echo 'Hello I am here...' "
      }
    }
    stage("prepare artifact"){
      steps{
        sh "make build"
      }
    }
    
  }
}
