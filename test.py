import fpdf
from fpdf import FPDF
import time
import pandas as pd
import matplotlib.pyplot as plt
import dataframe_image as dfi
import numpy as np
 
def generate_matplotlib_stackbars(df, filename):
    
    # Create subplot and bar
    fig, ax = plt.subplots()
    ax.plot(df['Year of Release'].values, df['Total Sales'].values, color="#E63946", marker='D') 

    # Set Title
    ax.set_title('Heicoders Academy Annual Sales', fontweight="bold")

    # Set xticklabels
    ax.set_xticklabels(df['Year of Release'].values, rotation=90)
    plt.xticks(df['Year of Release'].values)

    # Set ylabel
    ax.set_ylabel('Total Sales (USD $)') 

    # Save the plot as a PNG
    plt.savefig(filename, dpi=300, bbox_inches='tight', pad_inches=0)
    
    plt.show()
    
def generate_matplotlib_piechart(df, filename):
    
    # Pie chart, where the slices will be ordered and plotted counter-clockwise:
    labels = ["NA Sales", "EU Sales", "JP Sales", "Other Sales", "Global Sales"]
    sales_value = df[["NA Sales", "EU Sales", "JP Sales", "Other Sales", "Global Sales"]].tail(1)
    
    # Colors
    colors = ['#E63946','#F1FAEE','#A8DADC','#457B9D','#1D3557', '#9BF6FF']
    
    # Create subplot
    fig, ax = plt.subplots()
    
    # Generate pie chart
    ax.pie(sales_value, labels=labels, autopct='%1.1f%%', startangle=90, colors = colors)
    ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    
    # Set Title
    ax.set_title('Heicoders Academy 2016 Sales Breakdown', fontweight="bold")
    
    # Save the plot as a PNG
    plt.savefig(filename, dpi=300, bbox_inches='tight', pad_inches=0)
    
    plt.show()
    
def color_pos_neg_value(value):
    if value < 0:
        color = 'red'
    elif value > 0:
        color = 'green'
    else:
        color = 'black'
    return 'color: %s' % color

np.random.seed(24)
df = pd.DataFrame({'A': np.linspace(1, 10, 10)})
df = pd.concat([df, pd.DataFrame(np.random.randn(10, 4), columns=list('BCDE'))],
               axis=1)
df.iloc[3, 3] = np.nan
df.iloc[0, 2] = np.nan

print(df)

df.at[1, 1] = None
# Add styles
def draw_color_cell(x,color):
    color = f'background-color:{color}'
    return color
 

styled_df = df.style.format({'Year of Release': "{:.0f}",
                      'NA Sales': "{:.2f}",
                      'EU Sales': "{:.2f}",
                      'JP Sales': "{:.2f}",
                      'Other Sales': "{:.2f}",
                      'Global Sales': "{:.2f}",
                      'Total Sales': "{:.2f}",
                      'Sales Pct Change': "{:.2f}%",
                     }).hide_index().bar(subset=["Total Sales",], color='lightgreen').applymap(color_pos_neg_value, subset=['Sales Pct Change'])

generate_matplotlib_stackbars(df, 'resources/heicoders_annual_sales.png')

df.head()
  
print(df)