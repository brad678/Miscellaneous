

library(readr)
library(tidyr)
library(dplyr)
library(smooth)
library(TTR)
library(forecast)

train_1 <- read_csv("train_1.csv")
key_1 <- read_csv("key_1.csv")
samp_sub1 <- read_csv("sample_submission_1.csv")





key_1_upd <- separate(key_1,"Page",c("Page","Date"),sep=-11)
key_1_upd$Page <- substr(key_1_upd$Page, 1,nchar(key_1_upd$Page)-1) 


for(i in 1:nrow(train1))
{
  #creating data in proper format
  t1 <- as.data.frame(t(train_1[i,2:551]))
  
  #time series - daily
  x <- ts(t1$V1, frequency=365,start=c(2015,182))
  
  #sma model
  SMA_test <- sma(tsclean(x), h=60,silent="graph")
  
  #building output data
  idx <- which(train_1$Page[i]==key_1_upd)[1]
  dir <- "C:\\Users\\Srimala Bharadwaj\\Desktop\\Data science\\AV\\JetRail"
  test_op <- data.frame(Id=key_1_upd[idx:(idx+59),]$Id,Visits=SMA_test$forecast)
  colnames(test_op) <- c("Id","Visits")
  write_csv(test_op, file.path(dir, "smadly.csv"), col_names=ifelse(i %in% 1, TRUE, FALSE), append=TRUE)
  cat(i,";")
}




t1 <- as.data.frame(t(train_1[1,2:551]))
#time series - daily
x <- ts(t1$V1, frequency=365,start=c(2015,182))
#sma model
SMA_test <- sma(tsclean(x), h=60,silent="graph")
#building output data
idx <- which(train_1$Page[1]==key_1_upd)[1]
test_op <- data.frame(Id=key_1_upd[idx:(idx+59),]$Id,Visits=SMA_test$forecast)
colnames(test_op) <- c("Id","Visits")
write_csv(test_op, file.path(dir, "smadly.csv"), col_names=ifelse(i %in% 1, TRUE, FALSE), append=TRUE)
cat(1,";")


t1 <- as.data.frame(t(train_1[2,2:551]))
#time series - daily
x <- ts(t1$V1, frequency=365,start=c(2015,182))
#sma model
SMA_test <- sma(tsclean(x), h=60,silent="graph")
#building output data
idx <- which(train_1$Page[2]==key_1_upd)[1]
test_op <- data.frame(Id=key_1_upd[idx:(idx+59),]$Id,Visits=SMA_test$forecast)
colnames(test_op) <- c("Id","Visits")
write_csv(test_op, file.path(dir, "smadly.csv"), col_names=ifelse(i %in% 1, TRUE, FALSE), append=TRUE)
cat(2,";")





