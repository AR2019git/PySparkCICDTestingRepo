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
        echo 'This is the test stage - Test'
        sh 'pipenv run pytest -v --html=testreport.html'
      }
    }
    
  }

}
