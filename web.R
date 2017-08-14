

library(readr)
library(tidyr)
library(dplyr)
library(smooth)
library(TTR)
library(forecast)
library(data.table)
library(foreach)
library(doParallel) 

dir <- "D:\\Users\\bharadwaj\\Desktop\\DS\\Datasets\\Kaggle\\Web Traffic"
#dir <- "C:\\Users\\Srimala Bharadwaj\\Desktop\\Data science\\Kaggle\\Web Traffic"

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

dt_sum1 <- dt[,.(Visits = median(y[(length(y)-49):length(y)])) , by = Page]
setkey(dt_sum1, Page) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient


sub <- merge(key_1, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="median_7w.csv")
#######################################################################################################################################

dt_sum1 <- dt[,.(Visits = median(y[(length(y)-35):length(y)])) , by = Page]
setkey(dt_sum1, Page) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient


sub <- merge(key_1, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="median_5w.csv")
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

func_out_mean <- function(x, na.rm = TRUE, ...) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = na.rm, ...)
  H <- 1.5 * IQR(x, na.rm = na.rm)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  mean(y,na.rm=TRUE)
}


# calculate page mean removing outliers
dt_sum1 <- dt[,.(Visits = func_out_mean(sqrt(y[(length(y)-59):length(y)]))**2) , by = Page]
setkey(dt_sum1, Page) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient


sub <- merge(key_1, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="mean_out_2m_sqrt.csv")

#######################################################################################################################################
i <- 1
func_out_sma <- function(x, na.rm = TRUE, ...) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = na.rm, ...)
  H <- 1.5 * IQR(x, na.rm = na.rm)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  #mean(y,na.rm=TRUE)
  sma(tsclean(ts(na.omit(y))),h=1,silent="graph")$forecast
  cat(i,";")
  i <<- i+1 
}


# calculate page mean removing outliers
dt_sum1 <- dt[,.(Visits = func_out_sma(y[(length(y)-59):length(y)])) , by = Page]
setkey(dt_sum1, Page) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient


sub <- merge(key_1, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="mean_out_2m_sma.csv")

#######################################################################################################################################

#setup parallel backend to use many processors
cores=detectCores()
cl <- makeCluster(cores[1]) #not to overload your computer
registerDoParallel(cl)

strt <- Sys.time()

sma.model <- foreach(i=1:nrow(train_1), .combine=rbind,.packages=c("smooth","forecast","data.table")) %dopar% {
    #sma model
    idx <- which(train_1$Page[i]==key_1$Page)[1]
    test_op <- data.table(Id=key_1$Id[idx:(idx+59)],Visits=sma(tsclean(ts(as.data.table(t(train_1[i,2:551]))$V1, frequency=365,start=c(2015,182))), h=60,silent="graph")$forecast)
    test_op
  }

print(Sys.time()-strt)
#stop cluster
stopCluster(cl)
#######################################################################################################################################

#setup parallel backend to use many processors
cores=detectCores()
cl <- makeCluster(cores[1]) #not to overload your computer
#registerDoParallel(cl)
library(doSNOW)
registerDoSNOW(cl)

#set up progress bar to track progress
library(utils)
pb <- txtProgressBar(max = nrow(train_2), style=3)
progress <- function(n) setTxtProgressBar(pb, n)
opts <- list(progress = progress)

#start time of parallel process
strt <- Sys.time()

sma.model <- foreach(i=1:nrow(train_2), .combine=rbind,.packages=c("smooth","forecast","data.table"),.options.snow = opts) %dopar% {
  #sma model
  setTxtProgressBar(pb, i)
  
  idx <- which(train_2$Page[i]==key_1$Page)[1]
  test_op <- data.table(Id=key_1$Id[idx:(idx+59)],Visits=sma(tsclean(ts(as.data.table(t(train_2[i,2:551]))$V1, frequency=365,start=c(2015,182))), h=60,silent="graph")$forecast)
  #test_op
  fwrite(test_op,file="temp.csv",append=TRUE)
}

print(Sys.time()-strt)
#close progress bar
close(pb)
#stop cluster
stopCluster(cl)
#######################################################################################################################################
strt <- Sys.time()
sma.model <- data.table()
for(i in 1:nrow(train_1))
{
  idx <- which(train_1$Page[i]==key_1$Page)[1]
  test_op <- data.table(Id=key_1$Id[idx:(idx+59)],Visits=sma(ts(as.data.table(t(train_1[i,2:551]))$V1, frequency=365,start=c(2015,182)), h=60,silent="graph")$forecast)
  sma.model <- rbind(sma.model,test_op)
  if(i %% 500 == 0)
  {
    print(i)
  }
   
}
print(Sys.time()-strt)
colnames(sma.model) <- c('Id', 'Visits')
fwrite(sma.model,file="sma_model.csv")

#######################################################################################################################################

#setup parallel backend to use many processors
cores=detectCores()
cl <- makeCluster(cores[1]) #not to overload your computer
#registerDoParallel(cl)
library(doSNOW)
registerDoSNOW(cl)

#set up progress bar to track progress
library(utils)
pb <- txtProgressBar(max = nrow(train_2), style=3)
progress <- function(n) setTxtProgressBar(pb, n)
opts <- list(progress = progress)

#start time of parallel process
strt <- Sys.time()

sma.model <- foreach(s=1:2, .combine=rbind,.packages=c("smooth","forecast","data.table"),.options.snow = opts) %dopar% {
  #sma model
  for(i in 1:nrow)
  setTxtProgressBar(pb, i)
  
  idx <- which(train_2$Page[i]==key_1$Page)[1]
  test_op <- data.table(Id=key_1$Id[idx:(idx+59)],Visits=sma(tsclean(ts(as.data.table(t(train_2[i,2:551]))$V1, frequency=365,start=c(2015,182))), h=60,silent="graph")$forecast)
  test_op
}

print(Sys.time()-strt)
#close progress bar
close(pb)
#stop cluster
stopCluster(cl)








foreach(s=1:2, .combine=rbind,.packages=c("smooth","forecast","data.table"),.options.snow = opts) %dopar% {
  #sma model
  for(i in 1:nrow)
    setTxtProgressBar(pb, i)
  
  idx <- which(train_2$Page[i]==key_1$Page)[1]
  test_op <- data.table(Id=key_1$Id[idx:(idx+59)],Visits=sma(tsclean(ts(as.data.table(t(train_2[i,2:551]))$V1, frequency=365,start=c(2015,182))), h=60,silent="graph")$forecast)
  test_op
}


#########################################################################################################################################


med_2m <- fread("median_2m.csv")
med_7w <- fread("median_7w.csv")

med_2m_7w <- med_2m

med_2m_7w$Visits <- (med_2m$Visits+med_7w$Visits)/2

fwrite(med_2m_7w,file="median_2m_7w_avg.csv")

#########################################################################################################################################

train_1[is.na(train_1)] <- 0
sma_model <- fread("sma_model.csv")

idx <- 1

strt <- Sys.time()

for(i in 1:nrow(train_1))
{
  test_op <- data.table(Id=sma_model$Id[idx:(idx+59)],Visits=forecast(holt(ts(as.data.table(t(train_1[i,2:551]))$V1, frequency=365,start=c(2015,182))),60)$mean)
  idx <- idx+60
  fwrite(test_op,file="hw_model.csv",append=TRUE)
  if(i %% 500 == 0)
  {
    print(i)
  }
  
}
print(Sys.time()-strt)
#########################################################################################################################################


med_2m <- fread("median_2m.csv")
med_7w <- fread("median_7w.csv")

med_2m_7w <- med_2m
med_2m_7w$Visits1 <- med_7w$Visits

med_2m_7w$Visits2 <- ifelse(med_2m_7w$Visits>=med_2m_7w$Visits1, med_2m_7w$Visits, med_2m_7w$Visits1)
med_2m_7w[, Visits1:= NULL]
med_2m_7w[, Visits:= NULL]
setnames(med_2m_7w, "Visits2", "Visits")

fwrite(med_2m_7w,file="median_2m_7w_max.csv")
#########################################################################################################################################
med_2m_7w <- med_2m
med_2m_7w$Visits1 <- med_7w$Visits

med_2m_7w$Visits2 <- ifelse(med_2m_7w$Visits<=med_2m_7w$Visits1, med_2m_7w$Visits, med_2m_7w$Visits1)
med_2m_7w[, Visits1:= NULL]
med_2m_7w[, Visits:= NULL]
setnames(med_2m_7w, "Visits2", "Visits")

fwrite(med_2m_7w,file="median_2m_7w_min.csv")
#########################################################################################################################################
#train_1[is.na(train_1)] <- 0
train_1 <- fread("train_1.csv")
#sma_model <- fread("sma_model.csv")

idx <- 1

x <- vector("numeric",length=550)
s1 <- vector("numeric",length=2)
test_op <- data.table(Id   = character(60), Visits = numeric(60))


strt <- Sys.time()

for(i in 1:nrow(train_1))
{
  x <- tsclean(as.data.table(t(train_1[1,2:551]))$V1)
  #x <- ts(x, frequency=365,start=c(2015,182))
  p <- periodogram(x,plot = FALSE)
  s1 <- as.vector((1/p$freq)[order(-p$spec)[1:2]])
  test_op <- data.table(Id=sma_model$Id[idx:(idx+59)],Visits=forecast(tbats(x,seasonal.periods = c(s1[1],s1[2]),use.parallel = TRUE,num.cores=2),60)$mean)
  idx <- idx+60
  fwrite(test_op,file="tbats_model.csv",append=TRUE)
  if(i %% 500 == 0)
  {
    print(i)
  }
  
}
print(Sys.time()-strt)




#########################################################################################################################################



idx <- 1

x <- vector("numeric",length=550)
s1 <- vector("numeric",length=2)
test_op <- data.table(Id   = character(60), Visits = numeric(60))

strt <- Sys.time()

for(i in 1:nrow(train_1))
{
  x <- t(train_1[i,2:551])[,1]
  x[is.na(x)] <- 0
  #x <- tsclean(t(train_1[i,2:551])[,1])
  p <- periodogram(x,plot = FALSE)
  s1 <- (1/p$freq)[order(-p$spec)[1:2]]
  test_op[, Id:= sma_model$Id[idx:(idx+59)]]
  test_op[, Visits:=forecast(tbats(x,seasonal.periods = c(s1[1],s1[2])),60)$mean]
  idx <- idx+60
  fwrite(test_op,file="tbats_model.csv",append=TRUE)
  if(i %% 500 == 0)
  {
    print(i)
  }
  
  
  
}
print(Sys.time()-strt)




library(TSA)
library(doParallel)
library(itertools)

#idx <- 1

x <- vector("numeric",length=550)
s1 <- vector("numeric",length=2)
test_op <- data.table(Id   = character(60), Visits = numeric(60))

num_of_cores <- detectCores()
cl <- makePSOCKcluster(num_of_cores)
registerDoParallel(cl)

strt <- Sys.time()

r <- foreach(d=isplitRows(train_2, chunks=num_of_cores),
             .combine = cbind, .packages=c("forecast","TSA","data.table")) %dopar% {
               #result <- logisticRidge(admit~ gre + gpa + rank, data = d)
               #coefficients(result)
               for(i in 1:nrow(d))
               {
                 x <- t(d[i,2:551])[,1]
                 x[is.na(x)] <- 0
                 p <- periodogram(x,plot = FALSE)
                 s1 <- (1/p$freq)[order(-p$spec)[1:2]]
                 idx <- which(d$Page[i]==key_1$Page)[1]
                 test_op[, Id:= key_1$Id[idx:(idx+59)]]
                 #test_op[, Id:= sma_model$Id[idx:(idx+59)]]
                 test_op[, Visits:=forecast(tbats(x,seasonal.periods = c(s1[1],s1[2])),60)$mean]
                 #idx <- idx+60
                 #test_op
                 fwrite(test_op,file="tbats_model.csv",append=TRUE)
                    
               }
               
               
             }

print(Sys.time()-strt)
#stop cluster
stopCluster(cl)











