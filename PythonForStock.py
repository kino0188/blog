
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
import seaborn as sns

# 업데이트 내역
# FINVIZ에 없는 데이터 외에는 모든 데이터 리딩 확인
# 리트라이 횟수는 2번만
# PER순으로 정렬 
# TARGET값과 현재값 비교 - 비율 추가 
# 주요 지수들만 보이게 설정 PER, ROA, RSI 등 
# 엑셀로 저장되게 변경 시트, 나누는 과정 추가
# EPS 데이터 지수들 보이게 업데이트 , Discription 자료 추가
# PER 업, 다운 리미트 
# ROE 랭크 순으로 정렬 변경
# 날자별로 파일 저장 변경
# git push origin / git push -f origin
# TARGET PLICE 조건 추가 
# EPS 조건 추가

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

def color_negative_red(val):
    color = 'red' if val < 0 else 'black'
    return f'color: {color}'

def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)

# YAHOO 정보 가져오기
headers = {"User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"}

# 현재 시간 가져오기
filename = datetime.now().strftime("%Y-%m-%d")

# 폴더 만들기
if not os.path.exists('data/US_fs'):
    os.makedirs('data/US_fs')
if not os.path.exists('data/US_value'):
    os.makedirs('data/US_value')

# 가격 날자 지정
start = datetime(2021,11,15)
end = datetime(2021,11,15)

# 티커 불러오기
#US_ticker = pd.read_csv(r'C:\Users\io044\blog\data\US_ticker.csv', index_col=0)
US_ticker = pd.read_csv(r'C:\Users\io044\blog\data\US_ticker_test.csv', index_col=0)
US_ticker_back = pd.read_csv(r'C:\Users\io044\blog\data\US_ticker_back.csv', index_col=0)

#US_ticker = pd.read_csv(r'C:\Users\io044\blog\data\US_ticker_Contect.csv', index_col=0)
#US_ticker = pd.read_csv(r'C:\Users\io044\blog\data\US_ticker_SW.csv', index_col=0)

sum_value = pd.DataFrame({'' : [np.nan]})    
err_Frame = pd.DataFrame({'1':[np.nan,np.nan],'2':[np.nan,np.nan],'3':[np.nan,np.nan],'4':[np.nan,np.nan],'5':[np.nan,np.nan],'6':[np.nan,np.nan],'7':[np.nan,np.nan],'8':[np.nan,np.nan],'9':[np.nan,np.nan],'10':[np.nan,np.nan],
                          '11':[np.nan,np.nan],'12':[np.nan,np.nan],'13':[np.nan,np.nan],'14':[np.nan,np.nan],'15':[np.nan,np.nan],'16':[np.nan,np.nan],'17':[np.nan,np.nan],'18':[np.nan,np.nan]})
discription_Frame = pd.DataFrame({'Name':['Insider Own: 회사내부 주주 보유율% ( ex: 내부자는 일반 대중이 이용할 수 없는 중요한 금융 정보에 접근할 수 있는 모든 사람 또는 10% 이상의 기업에서 자본의 소유권을 가진 투자자로 정의됩니다)'
                                          , 'Insider Trans: 6개월 회사내부 주주 거래 변동%'
                                          , 'Inst Own: 기관 보유율%'
                                          , 'Inst Trans: 기관 보유율 6개월 변동%'
                                          , 'ROA: 총자산이익률 ( ex: 높다는 말은 보유하고 있는 자산으로. 많은 순이익을 벌어들였다는 뜻입니다)'
                                          , 'ROE: 자기자본이익률 ( ex: 타 기업들보다 낮으면 경영진이 무능하거나 그 업종이 불황이라는 뜻입니다)'
                                          , 'ROI: 투자자본수익률 ( ex: 높은 투자자본수익률은 투자가 투자비용 대비 좋은 성과를 낸다는 뜻입니다)'
                                          , 'Gross Margin: 매출총이익 ( ex: 매출에서 최소한의 비용을 제거하여 남은 큰의미의 이익을 의미합니다)'
                                          , 'Oper Margin: 영업이익 ( ex: 영업 마진이 높다는 것은 회사가 해당 사업을 유지하는 데 관련된 모든 관련 비용을 지불 할만큼 사업 운영에서 충분한 수익을 얻고 있음을 나타냅니다. 대부분의 비즈니스에서 15 % 이상의 영업 마진은 좋은 것으로 간주할 수 있습니다)'
                                          , 'Profit Margin: 순이익률 ( ex: $2를 얻기 위해 $1을 지출하면 순이익률은 50 %입니다.)'
                                          , 'Payout: 배당성향 ( ex: 일반적으로 배당금의 형태로 수익의 50 % 미만을 지급하는 회사는 안정적인 것으로 간주되며 회사는 장기적으로 수익을 올릴 가능성이 있습니다.)'
                                          , 'SMA200: 200일 이동평균선 수치'
                                          , 'Shs Outstand: 발행주식수-자사주'
                                          , 'EPS(ttm)는 Earning Per share의 약자로 주당 순이익을 의미합니다. ttm은 앞선 포스팅에서 언급드렸듯이 Trailing Twelve Months의 약자로 지난 12개월동안의 주당 순이익을 나타냅니다. EPS는 순이익을 발행주식수로 나눈 값입니다. ttm값이기 때문에 직전 4분기의 EPS값을 더한 값입니다. 이는 다시 말해서 지난 4분기의 순이익 합에서 발행주식수를 나누어 계산이 됩니다.'
                                          , 'EPS next Y는 다음년도 예상 EPS를 의미합니다. 이 지표가 클수록 기업의 향후 실적이 좋아 질것이라 시장에서 보고있습니다.'  
                                          , 'EPS next Q는 눈치채셨겠지만 다음분기의 예상 EPS입니다. 실적발표때 시장에서 예상하는 EPS보다 더 큰 실적을 낸다면, 주가에 긍정적인 영향을 줄수 있겠죠? 하지만 실적이 어닝이라고해서 반드시 주가가 뛰는 것은 아닙니다. 오히려 주가가 보합이거나 일부 조정을 받는 경우가 있는데, 이때는 실적보다 향후 예상실적이 낮게 평가될 때 발생합니다. 예를들어 21년 1분기 실적발표는 2분기의 중반에 보통 발표가 됩니다. 이때는 1분기의 실적보다 2분기가 절반가량 지나가는 시점의 2분기 예상 실적에 더 주가가 민감하게 반응합니다. '
                                          , 'EPS this Y 상기표에는 EPS this Y라고 되어 있으나, 제 사견으로는 EPS growth this year(주당 순이익 올해 성장률) 입니다. 올해 작년대비 EPS 성장률을 보여줍니다.'
                                          , 'EPS next Y 또한 EPS growth next year(주당순이익 내년 예상성장률)로서 내년도 예상되는 EPS 성장률입니다.'
                                          , 'EPS next 5Y와 EPS past 5Y는 각각 향후 5년간 예상되는 EPS 평균성장률과 과거 5년간 EPS 평균성장률을 보여줍니다.'
                                          , 'Sales past 5Y는 과거 5년간 매출 평균성장률을 보여줍니다.'
                                          , 'Sales Q/Q는 Quarterly revenue growth로서 분기별 매출 성장률을 의미합니다.'
                                          , 'EPS Q/Q 또한 Quarterly EPS growth로서 분기별 EPS 성장률을 의미합니다.'
                                          , 'Shs Float: 유통주식수'
                                          , 'Short Float:  공매도 주식수량 ( ex: 수치가 높을수록 투자자들이 장기적인 주식전망을 안좋게 보는 겁니다)'
                                          , 'Short Ratio: 공매도 비율'
                                          , 'Target Price: 애널리스트 목표주가'
                                          , '52W Range: 52주 평균주가'
                                          , 'RSI(14): 상대강도지수 ( ex: 40이하인 경후 침체상태라 부르며, 70이상인 경후 과열상태라 부릅니다)'
                                          , 'Rel Volume: 상대 거래량'
                                          , 'Avg Volume: 3개월 평균 거래량'
                                          , 'Volume: 거래량'
                                          , 'Perf Week: 1주간 주가변동률        ※ Month:달, Quarter:분기. Half Y:6개월, Year:년, YTD:연초 누계'
                                          , 'Price: 현재가'
                                          , 'P/E: 주가 수익률 버핏지수 [적정 14~30 - 50 이상이면 제외]'
                                          , 'EPS: 즉 주당순이익(EPS)은 기업이 1주당 얼마의 순이익을 냈는가를 나타내는 지표'
                                          ]})
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
#for i in tqdm(range(0, len(US_ticker_back))):   
for i in tqdm(range(0, 200)):   
    
    # 빈 데이터프레임 생성
    data_fs = pd.DataFrame({'' : [np.nan]})    
    data_value = pd.DataFrame({'' : [np.nan]})    
      
    is_key = 'Adj Close'

    time.sleep(0.01)  
    
    read_pass = True
    
    # 티커 선택
    try:
        name = US_ticker['symbol'][i]    
        sector = US_ticker['sector'][i]  
        industry = US_ticker['industry'][i]  
        country = US_ticker['country'][i]  
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
                data_value.iat[0,3] = sector
                data_value.iat[0,4] = industry
                data_value.iat[0,5] = country

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

# 첫번째 행 제거 
sum_value = sum_value.drop(index=0, axis=0)

# 컬럼명 변경 NAMING               
sum_value.rename(columns={0:'SYMBOL',1:'NAME',2:'INDEX',3:'SECTOR',4:'INDUSTRY',5:'COUNTRY',6:'BOOK/SH',7:'CASH/SH',8:'DEVIDED'
                            ,9:'DEVIDED %',10:'EMPLOYERR',11:'OPTIONABLE',12:'SHORTABLE',13:'RECOME',14:'P/E',15:'FORWAD P/E'
                            ,16:'PEG',17:'P/S',18:'P/B',19:'P/C',20:'P/FCF',
                            21:'Quick Ratio', 22:'Current Ratio', 23:'Debt/Eq', 24:'LT Debt/Eq', 25:'SMA20', 26:'EPS (ttm)', 27:'EPS next Y',
                            28:'EPS next Q', 29:'EPS this Y P', 30:'EPS next Y P', 31:'EPS next 5Y', 32:'EPS past 5Y', 33:'Sales past 5Y',
                            34:'Sales Q/Q', 35:'EPS Q/Q', 36:'Earnings', 37:'SMA50', 38:'Insider Own', 39:'Insider Trans',
                            40:'Inst Own',42:'ROA',43:'ROE',44:'ROI',53:'SHORT_RATIO',54:'TARGET PRICE',58:'RSI',71:'TARGET_PER',72:'CURR_PLICE'
                            ,73:'CHANGE',75:'AN_PLICE1',77:'AN_PLICE2',79:'AN_PLICE3'}, inplace=True)

# P/E 숫자만 입력 변경
sum_value[['P/E']] = sum_value['P/E'].str.extract('(\d+)')
# PE 데이터 스트링 인티저 변환 
sum_value[['P/E']] = sum_value[['P/E']].apply(pd.to_numeric)

# TAGER PLICE 숫자만 입력 변경
sum_value[['TARGET_PER']] = sum_value['TARGET_PER'].str.extract('(\d+)')
# PE 데이터 스트링 인티저 변환 
sum_value[['TARGET_PER']] = sum_value[['TARGET_PER']].apply(pd.to_numeric)

# FORWAD P/E 숫자만 입력 변경
sum_value[['FORWAD P/E']] = sum_value['FORWAD P/E'].str.extract('(\d+)')
# PE 데이터 스트링 인티저 변환 
sum_value[['FORWAD P/E']] = sum_value[['FORWAD P/E']].apply(pd.to_numeric)

# FORWAD P/E 숫자만 입력 변경
sum_value[['EPS this Y P']] = sum_value['EPS this Y P'].str.extract('(\d+)')
# PE 데이터 스트링 인티저 변환 
sum_value[['EPS this Y P']] = sum_value[['EPS this Y P']].apply(pd.to_numeric)

# FORWAD P/E 숫자만 입력 변경
sum_value[['EPS next Y P']] = sum_value['EPS next Y P'].str.extract('(\d+)')
# PE 데이터 스트링 인티저 변환 
sum_value[['EPS next Y P']] = sum_value[['EPS next Y P']].apply(pd.to_numeric)

# ROE 숫자만 입력 변경
sum_value[['ROE']] = sum_value['ROE'].str.extract('(\d+)')
# ROE 데이터 스트링 인티저 변환 
sum_value[['ROE']] = sum_value[['ROE']].apply(pd.to_numeric)

# 종목중 필터링 조건
subset_df = sum_value[ (((sum_value['P/E'] < 60) & (sum_value['P/E'] > 10)) | (sum_value['FORWAD P/E'] < 60) & (sum_value['FORWAD P/E'] > 10))      #PER 조건 현재 다음 년도
                    & (sum_value['ROE'] < 300) & (sum_value['ROE'] > 5)                                                                             #ROE 조건 
                    & (sum_value['EPS this Y P'] > 5)                                                                                               #EPS THIS 조건 5% 이상
                    & (sum_value['EPS next Y P'] > 5)                                                                                               #EPS NEXT 조건 5% 이상
                    & (sum_value['TARGET_PER'] > 2)]                                                                                                #Analist Tartget 조건 2% 이상

# 필요한 데이터만 선별 나머지 삭제 
subset_df = subset_df.loc[:,['SYMBOL','NAME','INDEX','SECTOR','INDUSTRY','COUNTRY','BOOK/SH','CASH/SH','DEVIDED','RECOME'
                             ,'EPS (ttm)', 'EPS next Y',
                            'EPS next Q', 'EPS this Y P', 'EPS next Y P', 'EPS next 5Y', 'EPS past 5Y', 'Sales past 5Y', 'Sales Q/Q', 'EPS Q/Q', 'Earnings'
                            ,'DEVIDED %','EMPLOYERR','OPTIONABLE','P/E','FORWAD P/E' ,'PEG','P/S','P/B','P/C','P/FCF','Inst Own','ROA','ROE','SHORT_RATIO'
                            ,'TARGET PRICE','RSI','ROI','TARGET_PER','CURR_PLICE','AN_PLICE1','AN_PLICE2','AN_PLICE3']]

# ROE 순으로 정렬 데이터가 없는곳은 최하위로 
subset_df.sort_values(by=['ROE'], axis = 0, inplace=True, ascending=False, na_position='first')

# 해당 날자 폴더 생성 
createFolder(r'C:\Users\io044\data\STOCK_RESULT_' + filename)

# 최종 결과 저장 & CSV 타입 저장
sum_value.to_csv(r'C:\Users\io044\data\STOCK_RESULT_' + filename + '\DATA_All.csv')    
subset_df.to_csv(r'C:\Users\io044\data\STOCK_RESULT_' + filename + '\DATA_Filterd.csv')    

# 엑셀 타입 저장
df2 = sum_value.copy()

with pd.ExcelWriter(r'C:\Users\io044\data\STOCK_RESULT_' + filename + '\DATA_Xls.xlsx') as writer:  
    sum_value.to_excel(writer, sheet_name='All_Data', engine='openpyxl')
    subset_df.to_excel(writer, sheet_name='Filter_Data', engine='openpyxl')
    discription_Frame.to_excel(writer, sheet_name='Discription', engine='openpyxl')
    
# HTML 저장
html = subset_df.to_html('StockAnalistedData.html', justify='center')
