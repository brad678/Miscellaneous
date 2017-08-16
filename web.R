

library(readr)
library(plyr)
library(tidyr)
library(dplyr)
library(smooth)
library(TTR)
library(forecast)
library(data.table)
library(foreach)
library(doParallel) 
library(caret)
library(xgboost)
library(itertools)
#library(Rmpi)

#dir <- "D:\\Users\\bharadwaj\\Desktop\\DS\\Datasets\\Kaggle\\Web Traffic"
dir <- "C:\\Users\\Srimala Bharadwaj\\Desktop\\Data science\\Kaggle\\Web Traffic"

train_1 <- fread("train_1.csv")

key_1 <- fread("key_1.csv")

#samp_sub1 <- fread("sample_submission_1.csv")


# extract a lists  of pages and dates
train.date.cols = names(train_1[,-1])

# reshape the training data into long format page, date and views
dt <- melt(train_1,
           d.vars = c("Page"),
           measure.vars = train.date.cols,
           variable.name = "ds",
           value.name = "y")
#######################################################################################################################################
# replace NAs with 0 and calculate page median
dt[is.na(y), y :=0]
dt_sum <- dt[,.(Visits = median(y)) , by = Page]
setkey(dt_sum, Page) #dt_sum data.table is sorted by Page. Accesses "by reference" and is memory efficient


#merge projection dates and key to create submission
key_1[, Page2:= substr(Page, 1, nchar(Page)-11)]
key_1[, Page:= NULL]
setnames(key_1, "Page2", "Page") #rename Page2 to Page
setkey(key_1, Page)  

sub <- merge(key_1, dt_sum, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="median.csv")

#######################################################################################################################################
dt_sum1 <- dt[,.(Visits = median(y[(length(y)-59):length(y)])) , by = Page]
setkey(dt_sum1, Page) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient


sub <- merge(key_1, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="median_2m.csv")


#######################################################################################################################################

# calculate page mean
dt_sum1 <- dt[,.(Visits = mean(y)) , by = Page]
setkey(dt_sum1, Page) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient


sub <- merge(key_1, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="mean.csv")

#######################################################################################################################################

func_out_mean <- function(x, na.rm = TRUE, ...) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = na.rm, ...)
  H <- 1.5 * IQR(x, na.rm = na.rm)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  mean(y,na.rm=TRUE)
}


# calculate page mean removing outliers
dt_sum1 <- dt[,.(Visits = func_out_mean(y)) , by = Page]
setkey(dt_sum1, Page) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient


sub <- merge(key_1, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="mean_out.csv")

#######################################################################################################################################

func_out_mean <- function(x, na.rm = TRUE, ...) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = na.rm, ...)
  H <- 1.5 * IQR(x, na.rm = na.rm)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  mean(y,na.rm=TRUE)
}


# calculate page mean removing outliers
dt_sum1 <- dt[,.(Visits = func_out_mean(y[(length(y)-59):length(y)])) , by = Page]
setkey(dt_sum1, Page) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient


sub <- merge(key_1, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="mean_out_2m.csv")

#######################################################################################################################################

#setup parallel backend to use many processors
cores=detectCores()
cl <- makeCluster(cores[1]) #not to overload your computer
registerDoParallel(cl)

system.time({
  
  sma.model <- foreach(i=1:nrow(train_1), .combine=rbind,.packages="smooth","forecast","data.table") %dopar% {
    #sma model
    idx <- which(train_1$Page[i]==key_1$Page)[1]
    test_op <- data.table(Id=key_1$Id[idx:(idx+59)],Visits=sma(tsclean(ts(as.data.table(t(train_1[i,2:551]))$V1, frequency=365,start=c(2015,182))), h=60,silent="graph")$forecast)
    test_op
  }
})

#stop cluster
stopCluster(cl)
#######################################################################################################################################



#setup parallel backend to use many processors
cores=detectCores()
cl <- makeCluster(cores[1]) #not to overload your computer
registerDoParallel(cl)


sma.model <- foreach(i=1:nrow(train_2), .combine=rbind,.packages="smooth","forecast","data.table") %dopar% {
  #sma model
  idx <- which(train_2$Page[i]==key_1$Page)[1]
  test_op <- data.table(Id=key_1$Id[idx:(idx+59)],Visits=sma(tsclean(ts(as.data.table(t(train_2[i,2:551]))$V1, frequency=365,start=c(2015,182))), h=60,silent="graph")$forecast)
  cat(i,";")
  test_op
}

#stop cluster
stopCluster(cl)






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

train_1[,-1][1, row.names:=train_1$Page]


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

#######################################################################################################################################
func_out_mean <- function(x, na.rm = TRUE, ...) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = na.rm, ...)
  H <- 1.5 * IQR(x, na.rm = na.rm)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  mean(y,na.rm=TRUE)
}


dt1 <- dt
dt1[, dow:= as.numeric(format(as.Date(ds),"%w"))]


# calculate page mean removing outliers
dt_sum1 <- dt1[,.(Visits = func_out_mean(y[(length(y)-59):length(y)])) , by = c("Page","dow")]
setkey(dt_sum1, Page, dow) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient

key_1_new <- fread("key_1.csv")
#merge projection dates and key to create submission
key_1_new[, Page2:= substr(Page, 1, nchar(Page)-11)]
key_1_new[, dow:= as.numeric(format(as.Date(substr(Page, nchar(Page)-9, nchar(Page))),"%w"))]
key_1_new[, Page:= NULL]
setnames(key_1_new, "Page2", "Page") #rename Page2 to Page
setkey(key_1_new, Page, dow)  



sub <- merge(key_1_new, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="mean_out_2m_dow.csv")

#######################################################################################################################################

func_out_median <- function(x, na.rm = TRUE, ...) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = na.rm, ...)
  H <- 1.5 * IQR(x, na.rm = na.rm)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  median(y,na.rm=TRUE)
}


# calculate page mean removing outliers
dt_sum1 <- dt1[,.(Visits = func_out_median(y[(length(y)-59):length(y)])) , by = c("Page","dow")]
setkey(dt_sum1, Page, dow) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient


sub <- merge(key_1_new, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="median_out_2m_dow.csv")
#######################################################################################################################################

func_out_mean <- function(x, na.rm = TRUE, ...) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = na.rm, ...)
  H <- 1.5 * IQR(x, na.rm = na.rm)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  mean(y,na.rm=TRUE)
}


dt1 <- dt
dt1[, dow:= as.numeric(format(as.Date(ds),"%w"))]
dt1[, dow:= ifelse((dow==0 | dow==6),0,1)]


# calculate page mean removing outliers
dt_sum1 <- dt1[,.(Visits = func_out_mean(y[(length(y)-59):length(y)])) , by = c("Page","dow")]
setkey(dt_sum1, Page, dow) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient

key_1_new <- fread("key_1.csv")
#merge projection dates and key to create submission
key_1_new[, Page2:= substr(Page, 1, nchar(Page)-11)]
key_1_new[, dow:= as.numeric(format(as.Date(substr(Page, nchar(Page)-9, nchar(Page))),"%w"))]
key_1_new[, dow:= ifelse((dow==0 | dow==6),0,1)]
key_1_new[, Page:= NULL]
setnames(key_1_new, "Page2", "Page") #rename Page2 to Page
setkey(key_1_new, Page, dow)  



sub <- merge(key_1_new, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="mean_out_2m_weekend.csv")

#######################################################################################################################################


# calculate page mean removing outliers
dt_sum1 <- dt1[,.(Visits = func_out_median(y[(length(y)-59):length(y)])) , by = c("Page","dow")]
setkey(dt_sum1, Page, dow) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient

sub <- merge(key_1_new, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="median_out_2m_weekend.csv")

#######################################################################################################################################


# calculate page mean removing outliers
dt_sum1 <- dt1[,.(Visits = median(y[(length(y)-59):length(y)])) , by = c("Page","dow")]
setkey(dt_sum1, Page, dow) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient

sub <- merge(key_1_new, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="median_2m_weekend.csv")

#######################################################################################################################################

train_1[is.na(train_1)] <- 0


strt <- Sys.time()

for(i in 1:nrow(train_1))
{
  #creating data in proper format
  t1 <- as.data.table(t(train_1[i,2:551]))
 
  #time series - daily
  x <- ts(t1$V1, frequency=365,start=c(2015,182))
  
  #building output data
  idx <- which(train_1$Page[i]==key_1$Page)[1]
  test_op <- data.table(Id=key_1[idx:(idx+59),]$Id,Visits=forecast(auto.arima(x),60)$mean)
  colnames(test_op) <- c("Id","Visits")
  fwrite(test_op,file="autoarima.csv",append=TRUE)
  
  if(i %% 500 == 0)
  {
    print(i)
  }
  
}
print(Sys.time()-strt)
#######################################################################################################################################

dt1 <- dt
dt1[, dow:= as.numeric(format(as.Date(ds),"%w"))]
dt1[, dow:= ifelse((dow==0 | dow==1 | dow==5 | dow==6),0,1)]


# calculate page mean removing outliers
dt_sum1 <- dt1[,.(Visits = median(y[(length(y)-59):length(y)])) , by = c("Page","dow")]
setkey(dt_sum1, Page, dow) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient

key_1_new <- fread("key_1.csv")
#merge projection dates and key to create submission
key_1_new[, Page2:= substr(Page, 1, nchar(Page)-11)]
key_1_new[, dow:= as.numeric(format(as.Date(substr(Page, nchar(Page)-9, nchar(Page))),"%w"))]
key_1_new[, dow:= ifelse((dow==0 | dow==1 | dow==5 | dow==6),0,1)]
key_1_new[, Page:= NULL]
setnames(key_1_new, "Page2", "Page") #rename Page2 to Page
setkey(key_1_new, Page, dow)  



sub <- merge(key_1_new, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="median_2m_nweekend.csv")

#######################################################################################################################################


#xgb


fitcontrol <- trainControl(method="cv",number=2,savePredictions='final')

#creating test

test<- fread("key_1.csv")
test[, Page2:= substr(Page, 1, nchar(Page)-11)]
test[, date:= substr(Page, nchar(Page)-9, nchar(Page))]
test[, Page:= NULL]
setnames(test, "Page2", "Page") #rename Page2 to Page

#creating model

strt <- Sys.time()

for(i in 9892:nrow(train_1))
{
  #creating individual train
  t1 <- as.data.frame(t(train_1[i,2:551]))
  setDT(t1, keep.rownames = TRUE)
  setnames(t1, old=c("rn","V1"), new=c("date","Visits"))
  
  train_x <- t1
  train_x[is.na(Visits), Visits :=0]
  
  #creating individual test
  #test_x <- test %>% filter(Page==train_1$Page[i])
  test_x <- test[Page==train_1$Page[i]]
  
  #create featues of train and test
  train_x$date<-as.Date(train_x$date,"%Y-%m-%d")
  test_x$date<-as.Date(test_x$date,"%Y-%m-%d")
  
  train_x$year<-as.numeric(format(train_x$date, "%Y"))
  test_x$year<-as.numeric(format(test_x$date, "%Y"))
  
  
  train_x$month<-as.numeric(format(train_x$date, "%m"))
  test_x$month<-as.numeric(format(test_x$date, "%m"))
  
  
  train_x$day<-as.numeric(format(train_x$date, "%d"))
  test_x$day<-as.numeric(format(test_x$date, "%d"))
  
  
  train_x$weekday<-as.factor(weekdays(train_x$date))
  test_x$weekday<-as.factor(weekdays(test_x$date))
  

  predictors<-c("year", "month", "day", "weekday.Friday", "weekday.Monday", "weekday.Saturday", "weekday.Sunday", "weekday.Thursday", "weekday.Tuesday", "weekday.Wednesday")
  
  dmy <- dummyVars(" ~ .", data = train_x, fullRank = F)
  x_train <- data.table(predict(dmy, newdata = train_x))
  
  
  dmy <- dummyVars(" ~ .", data = test_x[,c("year", "month", "day", "weekday")], fullRank = F)
  x_test <- data.table(predict(dmy, newdata = test_x[,c("year", "month", "day", "weekday")]))
  
  #xgb model
  set.seed(101)
  model_xgbi <- train(x_train[,..predictors],
                      x_train$Visits,
                      method = 'xgbTree',
                      verbose = FALSE,
                      trControl = fitcontrol)
  
  test_op <- data.table(Id=test_x$Id,Visits=predict(model_xgbi,x_test[,..predictors]))
  fwrite(test_op,file="xgb.csv",append = TRUE)
  if(i %% 200 == 0)
  {
    print(i)
  }
  
}


print(Sys.time()-strt)





























cores=detectCores()
cl <- makeCluster(cores[1]) #not to overload your computer
#library(doSNOW)
registerDoSNOW(cl)

#set up progress bar to track progress
#library(utils)
pb <- txtProgressBar(max = nrow(train_1), style=3)
progress <- function(n) setTxtProgressBar(pb, n)
opts <- list(progress = progress)

strt <- Sys.time()

#for(i in 1:nrow(train_2))
#{
xgb.model <- foreach(i=1:nrow(train_1),.packages=c("caret","data.table"),.options.snow = opts) %dopar% {

  setTxtProgressBar(pb, i)
  #creating individual train
  t1 <- as.data.frame(t(train_1[i,2:551]))
  setDT(t1, keep.rownames = TRUE)
  setnames(t1, old=c("rn","V1"), new=c("date","Visits"))
  
  train_x <- t1
  train_x[is.na(Visits), Visits :=0]
  
  #creating individual test
  #test_x <- test %>% filter(Page==train_2$Page[i])
  test_x <- test[Page==train_1$Page[i]]
  
  #create featues of train and test
  train_x$date<-as.Date(train_x$date,"%Y-%m-%d")
  test_x$date<-as.Date(test_x$date,"%Y-%m-%d")
  
  train_x$year<-as.numeric(format(train_x$date, "%Y"))
  test_x$year<-as.numeric(format(test_x$date, "%Y"))
  
  
  train_x$month<-as.numeric(format(train_x$date, "%m"))
  test_x$month<-as.numeric(format(test_x$date, "%m"))
  
  
  train_x$day<-as.numeric(format(train_x$date, "%d"))
  test_x$day<-as.numeric(format(test_x$date, "%d"))
  
  
  train_x$weekday<-as.factor(weekdays(train_x$date))
  test_x$weekday<-as.factor(weekdays(test_x$date))
  
  
  predictors<-c("year", "month", "day", "weekday.Friday", "weekday.Monday", "weekday.Saturday", "weekday.Sunday", "weekday.Thursday", "weekday.Tuesday", "weekday.Wednesday")
  
  dmy <- dummyVars(" ~ .", data = train_x, fullRank = F)
  x_train <- data.table(predict(dmy, newdata = train_x))
  
  
  dmy <- dummyVars(" ~ .", data = test_x[,c("year", "month", "day", "weekday")], fullRank = F)
  x_test <- data.table(predict(dmy, newdata = test_x[,c("year", "month", "day", "weekday")]))
  
  #xgb model
  set.seed(101)
  model_xgbi <- train(x_train[,..predictors],
                      x_train$Visits,
                      method = 'xgbTree',
                      verbose = FALSE,
                      trControl = fitcontrol)
  
  test_op <- data.table(Id=test_x$Id,Visits=predict(model_xgbi,x_test[,..predictors]))
  fwrite(test_op,file="xgb.csv",append = TRUE)
  #rm(train_x);rm(test_x);rm(x_train);rm(x_test);rm(dmy);rm(model_xgbi)
}

print(Sys.time()-strt)
#close progress bar
close(pb)
#stop cluster
stopCluster(cl)
registerDoSEQ()
gc()


  

  




cores=detectCores()
#cl <- makeCluster(cores[1]) #not to overload your computer
cl <- makePSOCKcluster(cores)
#library(doSNOW)
registerDoSNOW(cl)

#set up progress bar to track progress
#library(utils)
pb <- txtProgressBar(max = nrow(train_1[10295:145063,]), style=3)
progress <- function(n) setTxtProgressBar(pb, n)
opts <- list(progress = progress)

strt <- Sys.time()

#for(i in 1:nrow(train_2))
#{
xgb.model <- foreach(d=isplitRows(train_1[10295:145063,], chunks=num_of_cores),.packages=c("caret","data.table"),.options.snow = opts) %dopar% {
  
  
  
  for(i in 1:nrow(d))
  {
    setTxtProgressBar(pb, i)
    #creating individual train
    t1 <- as.data.frame(t(d[i,2:551]))
    setDT(t1, keep.rownames = TRUE)
    setnames(t1, old=c("rn","V1"), new=c("date","Visits"))
    
    train_x <- t1
    train_x[is.na(Visits), Visits :=0]
    
    #creating individual test
    #test_x <- test %>% filter(Page==train_2$Page[i])
    test_x <- test[Page==d$Page[i]]
    
    #create featues of train and test
    train_x$date<-as.Date(train_x$date,"%Y-%m-%d")
    test_x$date<-as.Date(test_x$date,"%Y-%m-%d")
    
    train_x$year<-as.numeric(format(train_x$date, "%Y"))
    test_x$year<-as.numeric(format(test_x$date, "%Y"))
    
    
    train_x$month<-as.numeric(format(train_x$date, "%m"))
    test_x$month<-as.numeric(format(test_x$date, "%m"))
    
    
    train_x$day<-as.numeric(format(train_x$date, "%d"))
    test_x$day<-as.numeric(format(test_x$date, "%d"))
    
    
    train_x$weekday<-as.factor(weekdays(train_x$date))
    test_x$weekday<-as.factor(weekdays(test_x$date))
    
    
    predictors<-c("year", "month", "day", "weekday.Friday", "weekday.Monday", "weekday.Saturday", "weekday.Sunday", "weekday.Thursday", "weekday.Tuesday", "weekday.Wednesday")
    
    dmy <- dummyVars(" ~ .", data = train_x, fullRank = F)
    x_train <- data.table(predict(dmy, newdata = train_x))
    
    
    dmy <- dummyVars(" ~ .", data = test_x[,c("year", "month", "day", "weekday")], fullRank = F)
    x_test <- data.table(predict(dmy, newdata = test_x[,c("year", "month", "day", "weekday")]))
    
    if(all(rep(x_train$Visits[1],550)==x_train$Visits)) {
      test_op <- data.table(Id=test_x$Id,Visits=rep(x_train$Visits[1],60))
      fwrite(test_op,file="xgb.csv",append = TRUE)
    } else {
      #xgb model
      set.seed(101)
      model_xgbi <- train(x_train[,..predictors],
                          x_train$Visits,
                          method = 'xgbTree',
                          verbose = FALSE,
                          trControl = fitcontrol)
      
      test_op <- data.table(Id=test_x$Id,Visits=predict(model_xgbi,x_test[,..predictors]))
      fwrite(test_op,file="xgb.csv",append = TRUE)
    }
  }
  
}

print(Sys.time()-strt)
#close progress bar
close(pb)
#stop cluster
stopCluster(cl)
registerDoSEQ()
gc()









































cores=detectCores()
#cl <- makeCluster(cores[1]) #not to overload your computer
cl <- makePSOCKcluster(cores)
#library(doSNOW)
#registerDoSNOW(cl)
registerDoParallel(cl)

#set up progress bar to track progress
#library(utils)
#pb <- txtProgressBar(max = nrow(train_2), style=3)
#progress <- function(n) setTxtProgressBar(pb, n)
#opts <- list(progress = progress)

strt <- Sys.time()

#for(i in 1:nrow(train_2))
#{
xgb.model <- foreach(d=isplitRows(train_2, chunks=cores),.combine = cbind,.packages=c("caret","data.table")) %dopar% {
  
  
  
  for(i in 1:nrow(d))
  {
    #setTxtProgressBar(pb, i)
    #creating individual train
    t1 <- as.data.frame(t(d[i,2:551]))
    setDT(t1, keep.rownames = TRUE)
    setnames(t1, old=c("rn","V1"), new=c("date","Visits"))
    
    train_x <- t1
    train_x[is.na(Visits), Visits :=0]
    
    #creating individual test
    #test_x <- test %>% filter(Page==train_2$Page[i])
    test_x <- test[Page==d$Page[i]]
    
    #create featues of train and test
    train_x$date<-as.Date(train_x$date,"%Y-%m-%d")
    test_x$date<-as.Date(test_x$date,"%Y-%m-%d")
    
    train_x$year<-as.numeric(format(train_x$date, "%Y"))
    test_x$year<-as.numeric(format(test_x$date, "%Y"))
    
    
    train_x$month<-as.numeric(format(train_x$date, "%m"))
    test_x$month<-as.numeric(format(test_x$date, "%m"))
    
    
    train_x$day<-as.numeric(format(train_x$date, "%d"))
    test_x$day<-as.numeric(format(test_x$date, "%d"))
    
    
    train_x$weekday<-as.factor(weekdays(train_x$date))
    test_x$weekday<-as.factor(weekdays(test_x$date))
    
    
    predictors<-c("year", "month", "day", "weekday.Friday", "weekday.Monday", "weekday.Saturday", "weekday.Sunday", "weekday.Thursday", "weekday.Tuesday", "weekday.Wednesday")
    
    dmy <- dummyVars(" ~ .", data = train_x, fullRank = F)
    x_train <- data.table(predict(dmy, newdata = train_x))
    
    
    dmy <- dummyVars(" ~ .", data = test_x[,c("year", "month", "day", "weekday")], fullRank = F)
    x_test <- data.table(predict(dmy, newdata = test_x[,c("year", "month", "day", "weekday")]))
    
    if(all(rep(x_train$Visits[1],550)==x_train$Visits)) {
      test_op <- data.table(Id=test_x$Id,Visits=rep(x_train$Visits[1],60))
      test_op
      #fwrite(test_op,file="xgb1.csv",append = TRUE)
    } else {
      #xgb model
      set.seed(101)
      model_xgbi <- train(x_train[,..predictors],
                          x_train$Visits,
                          method = 'xgbTree',
                          verbose = FALSE,
                          trControl = fitcontrol)
      
      test_op <- data.table(Id=test_x$Id,Visits=predict(model_xgbi,x_test[,..predictors]))
      test_op
      #fwrite(test_op,file="xgb1.csv",append = TRUE)
    }
  }
  
}

print(Sys.time()-strt)
#close progress bar
#close(pb)
#stop cluster
stopCluster(cl)
registerDoSEQ()
gc()










cores=detectCores()
cl <- makeCluster(cores[1]) #not to overload your computer
#library(doSNOW)
registerDoSNOW(cl)

#set up progress bar to track progress
#library(utils)
pb <- txtProgressBar(max = nrow(train_2), style=3)
progress <- function(n) setTxtProgressBar(pb, n)
opts <- list(progress = progress)

strt <- Sys.time()

#for(i in 1:nrow(train_2))
#{
xgb.model <- foreach(i=1:nrow(train_2),.packages=c("caret","data.table"),.options.snow = opts) %dopar% {
  
  setTxtProgressBar(pb, i)
  #creating individual train
  t1 <- as.data.frame(t(train_2[i,2:551]))
  setDT(t1, keep.rownames = TRUE)
  setnames(t1, old=c("rn","V1"), new=c("date","Visits"))
  
  train_x <- t1
  train_x[is.na(Visits), Visits :=0]
  
  #creating individual test
  #test_x <- test %>% filter(Page==train_2$Page[i])
  test_x <- test[Page==train_2$Page[i]]
  
  #create featues of train and test
  train_x$date<-as.Date(train_x$date,"%Y-%m-%d")
  test_x$date<-as.Date(test_x$date,"%Y-%m-%d")
  
  train_x$year<-as.numeric(format(train_x$date, "%Y"))
  test_x$year<-as.numeric(format(test_x$date, "%Y"))
  
  
  train_x$month<-as.numeric(format(train_x$date, "%m"))
  test_x$month<-as.numeric(format(test_x$date, "%m"))
  
  
  train_x$day<-as.numeric(format(train_x$date, "%d"))
  test_x$day<-as.numeric(format(test_x$date, "%d"))
  
  
  train_x$weekday<-as.factor(weekdays(train_x$date))
  test_x$weekday<-as.factor(weekdays(test_x$date))
  
  
  predictors<-c("year", "month", "day", "weekday.Friday", "weekday.Monday", "weekday.Saturday", "weekday.Sunday", "weekday.Thursday", "weekday.Tuesday", "weekday.Wednesday")
  
  dmy <- dummyVars(" ~ .", data = train_x, fullRank = F)
  x_train <- data.table(predict(dmy, newdata = train_x))
  
  
  dmy <- dummyVars(" ~ .", data = test_x[,c("year", "month", "day", "weekday")], fullRank = F)
  x_test <- data.table(predict(dmy, newdata = test_x[,c("year", "month", "day", "weekday")]))
  
  if(all(rep(x_train$Visits[1],550)==x_train$Visits)) {
    test_op <- data.table(Id=test_x$Id,Visits=rep(x_train$Visits[1],60))
    
    fwrite(test_op,file="xgb1.csv",append = TRUE)
  } else {
    #xgb model
    set.seed(101)
    model_xgbi <- train(x_train[,..predictors],
                        x_train$Visits,
                        method = 'xgbTree',
                        verbose = FALSE,
                        trControl = fitcontrol)
    
    test_op <- data.table(Id=test_x$Id,Visits=predict(model_xgbi,x_test[,..predictors]))
    
    fwrite(test_op,file="xgb1.csv",append = TRUE)
  }
}

print(Sys.time()-strt)
#close progress bar
close(pb)
#stop cluster
stopCluster(cl)
registerDoSEQ()
gc()

#######################################################################################################################################

train_1 <- fread("train_1.csv")

# extract a lists  of pages and dates
train.date.cols = names(train_1[,-1])

# reshape the training data into long format page, date and views
dt <- melt(train_1,
           d.vars = c("Page"),
           measure.vars = train.date.cols,
           variable.name = "date",
           value.name = "Visits")

rm(train_1)

# replace NAs with 0 and calculate page median
dt[is.na(Visits), Visits :=0]

#create featues of train and test
dt[, date:= as.Date(date,"%Y-%m-%d")]

dt[, year:= as.numeric(format(dt$date, "%Y"))]

dt[, month:= as.numeric(format(dt$date, "%m"))]

dt[, day:= as.numeric(format(dt$date, "%d"))]

dt[, weekday:= as.factor(weekdays(dt$date))]

dt[, date:= NULL]

dt[, access:= gsub(".*.org_(.*)_.*$","\\1",Page)]

dt[, agent:= gsub(".*_(.*)$","\\1",Page)]

dt[, access:= as.factor(access)]

dt[, agent:= as.factor(agent)]


#to know range of Visits
s <- summary(dt$Visits)
print(s)


group_Visits <- function(x)
{
  if(x<=s[[2]]) #1st quantile - 25%
  {
    return("Q1")
  } else if(x<=s[[3]]) { #median - 50%
    return("Q2")
  } else if(x<=s[[5]]) { #3rd quantile - 75%
    return("Q3")
  } else return("Q3+") #above 3rd quantile - 75% +
}

dt[, Visits_category:= sapply(Visits,group_Visits)]

dt[, Visits_category:= as.factor(Visits_category)]

table(dt$Visits_category)


set.seed(100)
index <- createDataPartition(dt$Visits_category, p=0.01, list=FALSE)
dt_train <- dt[index,]
set.seed(100)
dt_test <- sample_n(dt[-index,],100000,replace=TRUE)

rm(dt)


predictors<-c("year", "month", "day", "weekday.Friday", "weekday.Monday", "weekday.Saturday", "weekday.Sunday", "weekday.Thursday", "weekday.Tuesday", "weekday.Wednesday", "access.all-access", "access.desktop", "access.mobile-web", "agent.all-agents", "agent.spider")

dmy <- dummyVars(" ~ .", data = dt_train, fullRank = F)
x_train <- data.table(predict(dmy, newdata = dt_train))


#dmy <- dummyVars(" ~ .", data = test_x[,c("year", "month", "day", "weekday")], fullRank = F)
#x_test <- data.table(predict(dmy, newdata = test_x[,c("year", "month", "day", "weekday")]))














