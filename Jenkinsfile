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
  post { 
        always { 
            echo 'I will always say Hello again!'
            publishHTML([allowMissing: false, alwaysLinkToLastBuild: false, keepAll: false, reportDir: '', reportFiles: 'testreport.html', reportName: 'HTML Report', reportTitles: 'Test Execution  Report', useWrapperFileDirectly: true])
        }
    }

}
