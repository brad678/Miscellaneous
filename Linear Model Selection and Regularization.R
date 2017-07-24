
#linear regression

library(MASS)
library(ISLR)
data("Boston")

lm.fit <- lm(medv~lstat,data=Boston)
lm.fit

confint(lm.fit)

attach(Boston)

plot(lstat,medv)
abline(lm.fit,lwd=3)

plot(predict(lm.fit),residuals(lm.fit))

plot(predict(lm.fit),rstudent(lm.fit))
abline(h=3,col="red")

p <- sum(hatvalues(lm.fit))
n <- length(hatvalues(lm.fit))

plot(hatvalues(lm.fit))
abline(h=2*p/n,col="red")

lm.fit <- lm(medv~.,data=Boston)
summary(lm.fit)

#Non-linear Transformations of the Predictors

lm.fit2 <- lm(medv~lstat+I(lstat^2),data=Boston)
summary(lm.fit2)


lm.fit <- lm(medv~lstat,data=Boston)
anova(lm.fit,lm.fit2)


lm.fit5 <- lm(medv~poly(lstat,5),data=Boston)
summary(lm.fit5)


#comparision of various linear reg models using ANNOVA
anova(lm.fit,lm.fit2,lm.fit5)


#################################################################################################################################

#ridge regression - understanding

x=model.matrix (Salary???.,Hitters)
x=x[,-1]

y=Hitters$Salary[!is.na(Hitters$Salary)]

grid =10^ seq (10,-2, length =100)
ridge.mod =glmnet (x,y,alpha =0, lambda =grid)
ridge.mod

coef(ridge.mod)
dim(coef(ridge.mod))

ridge.mod$lambda[50]
coef(ridge.mod)[,50]


sqrt(sum(coef(ridge.mod)[ -1 ,50]^2) )

results <- NULL
for(i in 1:100)
{
  results[i] <- sqrt(sum(coef(ridge.mod)[ -1 ,i]^2) )
}


#ridge regression - train & test

set.seed (1)
train=sample (1: nrow(x), nrow(x)/2)
test=(- train )
y.test=y[test]

set.seed(1)
ridge.mod =glmnet (x[train ,],y[train],alpha =0, lambda =grid)
ridge.mod


set.seed (1)
cv.out =cv.glmnet (x[train ,],y[train],alpha =0)
plot(cv.out)

bestlam =cv.out$lambda.min
bestlam

ridge.pred=predict(ridge.mod ,s=bestlam ,newx=x[test ,])


library(Metrics)
mse(y.test,ridge.pred)

#lasso - train & test

set.seed(1)
cv.out =cv.glmnet (x[train ,],y[train],alpha =1)
plot(cv.out)

bestlam =cv.out$lambda.min
bestlam

set.seed(1)
lasso.mod =glmnet (x[train ,],y[train],alpha =1, lambda =grid)
lasso.pred=predict (lasso.mod ,s=bestlam ,newx=x[test ,])
mse(y.test,lasso.pred)

#################################################################################################################################

library(ISLR)
names(Hitters)

Hitters =na.omit(Hitters)

dim(Hitters)

library (leaps)

#best subset selection using 10 fold CV

k=10
set.seed (1)
folds=sample (1:k,nrow(Hitters ),replace =TRUE)
table(folds)

cv.errors =matrix (NA ,k,19, dimnames =list(NULL , paste (1:19) ))

for(j in 1:k)
  {
  best.fit=regsubsets(Salary???.,data=Hitters[folds !=j,],nvmax =19)
  for(i in 1:19)
    {
      coefi=coef(best.fit,id=i)
      test.mat=model.matrix(Salary???.,data=Hitters[folds ==j,])
      pred=test.mat [,names(coefi)] %*% coefi
      cv.errors[j,i]=mean((Hitters$Salary[folds ==j]-pred)^2)
    }
 }

mean.cv.errors =apply(cv.errors ,2, mean)

plot(mean.cv.errors ,type="b")

mean.cv.errors[which.min(mean.cv.errors)]

coef(best.fit, which.min(mean.cv.errors))


#forward stepwise selection using 10 fold CV

k=10

cv.errorsf =matrix (NA ,k,19, dimnames =list(NULL , paste (1:19) ))


for(j in 1:k)
{
  best.fitf=regsubsets(Salary???.,data=Hitters[folds !=j,],nvmax =19,method="forward")
  for(i in 1:19)
  {
    coefi=coef(best.fitf,id=i)
    test.mat=model.matrix(Salary???.,data=Hitters[folds ==j,])
    pred=test.mat [,names(coefi)] %*% coefi
    cv.errorsf[j,i]=mean((Hitters$Salary[folds ==j]-pred)^2)
  }
}

mean.cv.errorsf =apply(cv.errorsf ,2, mean)

plot(mean.cv.errorsf ,type="b")

mean.cv.errorsf[which.min(mean.cv.errorsf)]

coef(best.fitf, which.min(mean.cv.errorsf))



#backward stepwise selection using 10 fold CV

k=10

cv.errorsb =matrix (NA ,k,19, dimnames =list(NULL , paste (1:19) ))


for(j in 1:k)
{
  best.fitb=regsubsets(Salary???.,data=Hitters[folds !=j,],nvmax =19,method="backward")
  for(i in 1:19)
  {
    coefi=coef(best.fitb,id=i)
    test.mat=model.matrix(Salary???.,data=Hitters[folds ==j,])
    pred=test.mat [,names(coefi)] %*% coefi
    cv.errorsb[j,i]=mean((Hitters$Salary[folds ==j]-pred)^2)
  }
}

mean.cv.errorsb =apply(cv.errorsb ,2, mean)

plot(mean.cv.errorsb ,type="b")

mean.cv.errorsb[which.min(mean.cv.errorsb)]

coef(best.fitb, which.min(mean.cv.errorsb))

