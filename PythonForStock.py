from typing_extensions import runtime
from bs4.element import Comment
import pandas as pd
import numpy as np
import os
from requests.api import get
import yahoo_fin.stock_info as si
import pandas_datareader as web
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup as soup
from urllib.request import Request, urlopen
import time
from tqdm import tqdm
from datetime import datetime
from urllib.request import urlopen 
import plotly.express as px
import csv

'''
Insider Own: 회사내부 주주 보유율% ( ex: 내부자는 일반 대중이 이용할 수 없는 중요한 금융 정보에 접근할 수 있는 모든 사람 또는 10% 이상의 기업에서 자본의 소유권을 가진 투자자로 정의됩니다)
Insider Trans: 6개월 회사내부 주주 거래 변동%
Inst Own: 기관 보유율%
Inst Trans: 기관 보유율 6개월 변동%
ROA: 총자산이익률 ( ex: 높다는 말은 보유하고 있는 자산으로. 많은 순이익을 벌어들였다는 뜻입니다)
ROE: 자기자본이익률 ( ex: 타 기업들보다 낮으면 경영진이 무능하거나 그 업종이 불황이라는 뜻입니다)
ROI: 투자자본수익률 ( ex: 높은 투자자본수익률은 투자가 투자비용 대비 좋은 성과를 낸다는 뜻입니다)
Gross Margin: 매출총이익 ( ex: 매출에서 최소한의 비용을 제거하여 남은 큰의미의 이익을 의미합니다)
Oper Margin: 영업이익 ( ex: 영업 마진이 높다는 것은 회사가 해당 사업을 유지하는 데 관련된 모든 관련 비용을 지불 할만큼 사업 운영에서 충분한 수익을 얻고 있음을 나타냅니다. 대부분의 비즈니스에서 15 % 이상의 영업 마진은 좋은 것으로 간주할 수 있습니다)
Profit Margin: 순이익률 ( ex: $2를 얻기 위해 $1을 지출하면 순이익률은 50 %입니다.)
Payout: 배당성향 ( ex: 일반적으로 배당금의 형태로 수익의 50 % 미만을 지급하는 회사는 안정적인 것으로 간주되며 회사는 장기적으로 수익을 올릴 가능성이 있습니다.)
SMA200: 200일 이동평균선 수치
Shs Outstand: 발행주식수-자사주
Shs Float: 유통주식수
Short Float:  공매도 주식수량 ( ex: 수치가 높을수록 투자자들이 장기적인 주식전망을 안좋게 보는 겁니다)
Short Ratio: 공매도 비율
Target Price: 애널리스트 목표주가
52W Range: 52주 평균주가
52W High: 52주 최고가
52W Low: 52주 최저가
RSI(14): 상대강도지수 ( ex: 40이하인 경후 침체상태라 부르며, 70이상인 경후 과열상태라 부릅니다)
Rel Volume: 상대 거래량
Avg Volume: 3개월 평균 거래량
Volume: 거래량
Perf Week: 1주간 주가변동률        ※ Month:달, Quarter:분기. Half Y:6개월, Year:년, YTD:연초 누계
Beta: 증시베타  ( ex: 시간이 지남에 따라 시장보다 더 많이 변동하는 주식은 베타가 1.0 이상입니다. 주식이 시장보다 적게 움직이면 주식의 베타는 1.0 미만입니다. 높은 베타 주식은 더 위험하지만 더 높은 수익률을 끌어올수 있습니다)
ATR: 14일 변동성 ( ex: 값이 크면 변동성 높은것이고 작으면 변동성 작은 것입니다)
Volatility(week, month): 변동성 주,달  ( ex: 좋은 소식은 변동성이 증가하면 더 많은 돈을 벌 수있는 잠재력도 빠르게 증가한다는 것입니다. 나쁜 소식은 변동성이 높을수록 위험도 높아진다는 것입니다.)
Prev Close: 전날 종가
Price: 현재가
Change: 하루 변동%

((TARGET - 현재값 ) / 현재값) / 100 - 추가 목표가

엑셀 조건부서식으로 컬러 조절하기 
'''

# FINVIZ에 없는 데이터 외에는 모든 데이터 리딩 확인
# 리트라이 횟수는 2번만
# PER순으로 정렬 
# TARGET값과 현재값 비교 - 비율 추가 
# 주요 지수들만 보이게 설정 PER, ROA, RSI 등 

class ErrCount:
    def __init__(self):
        self.count = 0
    def get_count(self):
        self.count += 1  
        if self.count > 5:
            return 1
        else:
            self.count = 0
            return 2
    def reset_count(self):
        self.count = 0
        
def get_fundamentals(symbol):
    try:
        # Set up scraper
        url = ("http://finviz.com/quote.ashx?t=" + symbol.lower())
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req).read()
        html = soup(webpage, "html.parser")
        
        # Find fundamentals table
        fundamentals = pd.read_html(str(html), attrs = {'class': 'snapshot-table2'})[0]
        
        # Clean up fundamentals dataframe
        fundamentals.columns = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11']
        colOne = []
        colLength = len(fundamentals)
        for k in np.arange(0, colLength, 2):
            colOne.append(fundamentals[f'{k}'])
        attrs = pd.concat(colOne, ignore_index=True)
    
        colTwo = []
        colLength = len(fundamentals)
        for k in np.arange(1, colLength, 2):
            colTwo.append(fundamentals[f'{k}'])
        vals = pd.concat(colTwo, ignore_index=True)

        fundamentals = pd.DataFrame()
        fundamentals['Attributes'] = attrs
        fundamentals['Values'] = vals
        fundamentals = fundamentals.set_index('Attributes')
        fundamentals = fundamentals.T
        fundamentals = fundamentals.reset_index()
        fundamentals = fundamentals.append(fundamentals, ignore_index=True)
        fundamentals = fundamentals.drop(index=0, axis=0)
        
        return fundamentals

    except Exception as e:
        
        return 'ERR'        
    
def get_analists(symbol):
    try:
        time.sleep(0.1)  

        url = ("http://finviz.com/quote.ashx?t=" + symbol.lower())
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req).read()
        html = soup(webpage, "html.parser")
        
        # Find news table
        try:
            analist = pd.read_html(str(html), attrs = {'class': 'fullview-ratings-outer'})[0]
        except:
            return 'ERR' 

        analistQ = analist.T
        analistR = analistQ.iloc[:,0:0]
        
        # first Comment
        analist = analistQ.iloc[:,0]

        analist = analist.reset_index()
        analist = analist.rename({0 : 'Comment'}, axis=1)
        df = analist["Comment"].str.split('$')
        
        analist = analist.iloc[:,1]
        
        analistR = pd.concat([analistR, analist], ignore_index=True, axis=1)
        df = df.str.get(1)
        analistR["plice1"] = df.str.extract('(\d+)')           #숫자만 입력
        
        # Second Comment
        analist = analistQ.iloc[:,1]
                
        analist = analist.reset_index()
        analist = analist.rename({1 : 'Comment'}, axis=1)
        df = analist["Comment"].str.split('$')
        
        analist = analist.iloc[:,1]
        
        analistR = pd.concat([analistR, analist], ignore_index=True, axis=1)
        df = df.str.get(1)
        analistR["plice2"] = df.str.extract('(\d+)')           #숫자만 입력
        
        # Therd Comment
        analist = analistQ.iloc[:,2]
                
        analist = analist.reset_index()
        analist = analist.rename({2 : 'Comment'}, axis=1)
        df = analist["Comment"].str.split('$')
        
        analist = analist.iloc[:,1]
        
        analistR = pd.concat([analistR, analist], ignore_index=True, axis=1)
        df = df.str.get(1)
        analistR["plice3"] = df.str.extract('(\d+)')           #숫자만 입력

        analistR = analistR.append(analistR, ignore_index=True)
        analistR = analistR.drop(index=0, axis=0)
        
        return analistR

    except Exception as e:
        return 'ERR'    
    
def draw_color_at_nan(x,color):
    if pd.isna(x):
        color = f'background-color:{color}'
        return color
    else:
        return ''

def draw_color_at_maxmum(x,color):
    color = f'background-color:{color}'
    is_max = x == x.max()
    return [color if b else '' for b in is_max]    

# YAHOO 정보 가져오기
headers = {"User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"}

# 폴더 만들기
if not os.path.exists('data/US_fs'):
    os.makedirs('data/US_fs')
if not os.path.exists('data/US_value'):
    os.makedirs('data/US_value')

# 가격 날자 지정
start = datetime(2021,11,15)
end = datetime(2021,11,15)

# 티커 불러오기
US_ticker = pd.read_csv(r'C:\Users\io044\blog\data\US_ticker.csv', index_col=0)
#US_ticker = pd.read_csv(r'C:\Users\io044\blog\data\US_ticker_Contect.csv', index_col=0)
#US_ticker = pd.read_csv(r'C:\Users\io044\blog\data\US_ticker_SW.csv', index_col=0)
US_ticker_back = pd.read_csv(r'C:\Users\io044\blog\data\US_ticker_back.csv', index_col=0)

sum_value = pd.DataFrame({'' : [np.nan]})    

err_Frame = pd.DataFrame({'1':[np.nan,np.nan],'2':[np.nan,np.nan],'3':[np.nan,np.nan],'4':[np.nan,np.nan],'5':[np.nan,np.nan],'6':[np.nan,np.nan],'7':[np.nan,np.nan],'8':[np.nan,np.nan],'9':[np.nan,np.nan],'10':[np.nan,np.nan],
                          '11':[np.nan,np.nan],'12':[np.nan,np.nan],'13':[np.nan,np.nan],'14':[np.nan,np.nan],'15':[np.nan,np.nan],'16':[np.nan,np.nan],'17':[np.nan,np.nan],'18':[np.nan,np.nan]})
err_Frame = err_Frame.drop(index=0, axis=0)

# FINVIZ 데이터 가져오기
pd.set_option('display.max_colwidth', 25)

# FINVIZ 종료
finviz_err = 0
retry_count = 0

eCnt = ErrCount()
eCnt.__init__()

print("START GET STOCKS DATA !!")

# for loop
for i in tqdm(range(0, len(US_ticker_back))):   
#for i in tqdm(range(0, 200)):   
    
    # 빈 데이터프레임 생성
    data_fs = pd.DataFrame({'' : [np.nan]})    
    data_value = pd.DataFrame({'' : [np.nan]})    
      
    is_key = 'Adj Close'

    time.sleep(0.01)  
    
    read_pass = True
    
    # 티커 선택
    try:
        name = US_ticker['symbol'][i]    
        fullname = US_ticker['name'][i]  
    except:
        read_pass = False
    
    if read_pass == True:
        while True:
        # 오류 발생 시 이를 무시하고 다음 루프로 진행
        # 오류 발생 시 재시도하는걸로 변경할것 
            try:
                # FINVIZ PRICE 데이터 받아오기
                finvizData1 = get_fundamentals(name)
                
                # FINVIZ 데이터 병합 데이터가 있을때(에러없음)
                if finvizData1 is 'ERR':
                    print("NONE PRICE DATA")
                else:    
                    try:
                        # FINVIZ 목표가 증가율을 구해서 추가한다.    
                        curr_Plice = float(finvizData1.loc['1':,'Price'])
                        Target_Price = float(finvizData1.loc['1':,'Target Price'])
                        TargetPer = ((Target_Price - curr_Plice) / curr_Plice) * 100.0
                        
                        # Prev Close 에 담아준다
                        finvizData1.loc['1':,'Prev Close'] = TargetPer
                    
                        # Price PER 합치기 
                        data_value = pd.concat([data_value, finvizData1], ignore_index=True, axis=1)
                            
                    except:
                        data_value = pd.concat([data_value, finvizData1], ignore_index=True, axis=1)

                # FINVIZ ANALIST 데이터 받아오기 
                finvizData = get_analists(name)
  
                # FINVIZ 데이터 병합 데이터가 있을때(에러없음)
                if finvizData is 'ERR':
                    print("NONE ANALIST DATA")
                else:    
                    data_value = pd.concat([data_value, finvizData], ignore_index=True, axis=1)
  
                data_value = data_value.drop(index=0, axis=0)    
                
                data_value.iat[0,0] = name
                data_value.iat[0,1] = fullname

                #행 추가 append
                sum_value = sum_value.append(data_value, ignore_index=True)
                print(sum_value)
                
            except:
                time.sleep(1.0)  

                if retry_count < 2:
                    # 오류 발생시 해당 종목명을 저장하고 다음 루프로 이동   
                    print("\n - RE TRY! " + name + "")
                    retry_count = retry_count+1
                    continue
                else:
                    print("\n - PASS! " + name + "")
                    retry_count = 0
                    break
                
            break

    #슬립 대기 
    time.sleep(0.005)     
    
    #CSV 파일 저장 
    sum_value.to_csv(r'C:\Users\io044\data\SoftwareAnalistSum2.csv')    

#첫번째 행 제거 
sum_value = sum_value.drop(index=0, axis=0)

#컬럼명 변경                 
sum_value.rename(columns={0:'SYMBOL',1:'NAME',2:'INDEX',3:'MARKET CAP',4:'INCOME',5:'SALES',6:'BOOK/SH',7:'CASH/SH',8:'DEVIDED'
                            ,9:'DEVIDED %',10:'EMPLOYERR',11:'OPTIONABLE',12:'SHORTABLE',13:'RECOME',14:'P/E',15:'FORWAD P/E'
                            ,16:'PEG',17:'P/S',18:'P/B',19:'P/C',40:'Inst Own',42:'ROA',43:'ROE',44:'ROI',53:'SHORT_RATIO',54:'TARGET PRICE',58:'RSI',71:'TARGET_PER',72:'CURR_PLICE'
                            ,73:'CHANGE',75:'AN_PLICE1',77:'AN_PLICE2',79:'AN_PLICE3'}, inplace=True)

#숫자만 입력 변경
sum_value[['P/E']] = sum_value['P/E'].str.extract('(\d+)')

# PE 데이터 스트링 인티저 변환 
sum_value[['P/E']] = sum_value[['P/E']].apply(pd.to_numeric)

# PE 데이터 100 이하만 추출 
underPE100 = sum_value['P/E'] <= 100

# 두가지 조건를 동시에 충족하는 데이터를 필터링하여 새로운 변수에 저장합니다. (AND)
subset_df = sum_value[underPE100]

# 필요한 데이터만 선별 나머지 삭제 
subset_df = subset_df.loc[:,['SYMBOL','NAME','INDEX','MARKET CAP','INCOME','SALES','BOOK/SH','CASH/SH','DEVIDED'
                            ,'DEVIDED %','EMPLOYERR','OPTIONABLE','P/E','FORWAD P/E'
                            ,'PEG','P/S','P/B','P/C','Inst Own','ROA','ROE','SHORT_RATIO','TARGET PRICE','RSI','ROI','TARGET_PER','CURR_PLICE','CHANGE'
                            ,'AN_PLICE1','AN_PLICE2','AN_PLICE3']]

#PER 순으로 정렬 데이터가 없는곳은 최하위로 
subset_df.sort_values(by=['P/E'], axis = 0, inplace=True, ascending=False, na_position='first')

# 없는 데이터 색상 변경
subset_df.style.applymap(draw_color_at_nan,color='#ff9090')
subset_df.style.apply(draw_color_at_maxmum,color='#ff9090',subset=['P/E'],axis=1)

#최종 결과 저장 
sum_value.to_csv(r'C:\Users\io044\data\SoftwareAnalistSum_Result.csv')    
subset_df.to_csv(r'C:\Users\io044\data\SoftwareAnalistSum_Result_filtering.csv')    

# HTML 저장
html = subset_df.to_html('StockAnalistedData.html', justify='center')
