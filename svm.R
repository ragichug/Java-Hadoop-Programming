svm <- read.csv("dataset_chunk1.csv")
str(svm)
library(kernlab)
svm_train <- svm [1:17000, ]
svm_test <- svm[17001:19999, ]
svm_classifier <- ksvm(LABEL ~ ., data = svm_train,
                       kernel = "vanilladot")
svm_predictions <- predict(svm_classifier, svm_test)
table(svm_test$LABEL, svm_predictions)
agreement <- svm_predictions == svm_test$LABEL
table(agreement)

