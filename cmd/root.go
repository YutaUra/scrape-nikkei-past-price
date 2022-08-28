/*
Copyright © 2022 YutaUra <yuuta3594@outlook.jp>
*/
package cmd

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/PuerkitoBio/goquery"
	"github.com/saintfish/chardet"
	"github.com/spf13/cobra"
	"golang.org/x/net/html/charset"
	"golang.org/x/sync/semaphore"
	"golang.org/x/text/transform"
)

var ctx = context.TODO()

func openInputFile(path string) ([]byte, error) {
	// read file
	bytes, err := os.ReadFile(path)
	if err != nil {
		switch {
		case errors.Is(err, syscall.ENOENT):
			return nil, fmt.Errorf("入力されたファイルが見つかりませんでした: %s", path)
		default:
			return nil, err
		}
	}

	// encode detect
	d := chardet.NewTextDetector()
	r, err := d.DetectBest(bytes)
	if err != nil {
		return nil, err
	}
	e, _ := charset.Lookup(r.Charset)
	if e == nil {
		return nil, fmt.Errorf("入力ファイルのエンコーディングが不明です: %s", r.Charset)
	}
	decodeStr, _, err := transform.Bytes(
		e.NewDecoder(),
		bytes,
	)
	if err != nil {
		return nil, err
	}
	return decodeStr, nil
}

func readCsv(sem *semaphore.Weighted, src []byte, skipHeader int, action func(number int, name string) error) error {
	r := csv.NewReader(bytes.NewReader(src))
	for i := 0; i < skipHeader; i++ {
		_, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	i := 0
	for {
		j := i + skipHeader
		record, err := r.Read()
		i++
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		companyName := record[0]

		if err := sem.Acquire(ctx, 1); err != nil {
			log.Printf("Failed to acquire semaphore: %v", err)
			return err
		}
		go func() {
			defer sem.Release(1)
			err = action(j, companyName)
		}()

		if err != nil {
			return err
		}
	}

	return nil
}

type ScrapeResult struct {
	CompanyName, StockCode string
	// 2013 ~ 2022
	Price2013, Price2014, Price2015, Price2016, Price2017, Price2018, Price2019, Price2020, Price2021, Price2022 float64
}

func getStockCode(companyName string) (string, error) {
	resp, err := http.Get(fmt.Sprintf("https://www.nikkei.com/nkd/search?searchKeyword=%s", url.QueryEscape(companyName)))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("日経のサイトでステータスコード %d が返りました", resp.StatusCode)
	}
	code := resp.Request.URL.Query().Get("scode")
	if code != "" {
		return code, nil
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return "", err
	}
	doc.Find(".m-companyList_item_data_name").EachWithBreak(func(i int, s *goquery.Selection) bool {
		if strings.TrimSpace(s.Text()) == companyName {
			href, exists := s.Attr("href")
			if exists {
				value, err := url.ParseQuery(strings.Split(href, "?")[1])
				if err != nil {
					return false
				}
				code = value.Get("scode")
			}
			return false
		}
		return true
	})

	return code, nil
}

func searchPastStock(companyName string) (ScrapeResult, error) {
	result := ScrapeResult{CompanyName: companyName}

	code, err := getStockCode(companyName)
	if err != nil {
		return result, err
	}
	if code == "" {
		log.Printf("該当する企業が見つかりませんでした: %s", companyName)
		return result, nil
	}
	result.StockCode = code

	// search https://www.nikkei.com/nkd/company/history/yprice/?scode=8304
	resp, err := http.Get(fmt.Sprintf("https://www.nikkei.com/nkd/company/history/yprice?scode=%s", url.QueryEscape(code)))
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return result, fmt.Errorf("日経のサイトでステータスコード %d が返りました", resp.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return result, err
	}

	doc.Find(".m-headline").Each(func(_ int, s *goquery.Selection) {
		if s.Find(".m-headline_text").Text() != "年間高安（過去10年）" {
			return
		}
		s.Next().Find("tr").Each(func(_ int, s *goquery.Selection) {
			if s.Find("th").First().Text() == "年" {
				return
			}
			// 年を取得
			year := strings.TrimSpace(s.Find("th").First().Text())
			// 終値を取得
			priceWithDate := strings.TrimSpace(s.Find("td:nth-child(5)").Text())
			priceRaw := strings.ReplaceAll(strings.Split(priceWithDate, "(")[0], ",", "")
			price, err := strconv.ParseFloat(priceRaw, 64)
			if err != nil {
				log.Printf("年 %s の終値が正しく取得できませんでした: %s", year, priceRaw)
				return
			}

			switch year {
			case "2013年":
				result.Price2013 = price
			case "2014年":
				result.Price2014 = price
			case "2015年":
				result.Price2015 = price
			case "2016年":
				result.Price2016 = price
			case "2017年":
				result.Price2017 = price
			case "2018年":
				result.Price2018 = price
			case "2019年":
				result.Price2019 = price
			case "2020年":
				result.Price2020 = price
			case "2021年":
				result.Price2021 = price
			case "2022年":
				result.Price2022 = price
			}
		})
	})

	return result, nil
}

var rootCmd = &cobra.Command{
	Use:   "日本経済新聞 株価スクレイピングツール",
	Short: "日本経済新聞のサイトから企業の過去の株価をスクレイピングする CLI です",
	Long: `日本経済新聞のサイトから企業の過去の株価をスクレイピングする CLI です

具体的な利用方法:
  scrape-nikkei-past-price --input 1001人以上プライム.csv --output output.csv --concurrency 10

注意点:
  - パフォーマンス向上のため、生成される csv ファイルは input ファイルと同じ順番で出力されません。
    input ファイルでの列番号は index 列に保存してあるため、順番が重要な場合は適宜変更してください。`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	RunE: func(cmd *cobra.Command, args []string) error {
		// get flags
		input, err := cmd.Flags().GetString("input")
		if err != nil {
			return err
		}
		header, err := cmd.Flags().GetInt("header")
		if err != nil {
			return err
		}
		output, err := cmd.Flags().GetString("output")
		if err != nil {
			return err
		}
		concurrency, err := cmd.Flags().GetInt64("concurrency")
		if err != nil {
			return err
		}

		// open input file
		inputSrc, err := openInputFile(input)
		if err != nil {
			return err
		}

		// create output file
		f, err := os.Create(output)
		if err != nil {
			return err
		}
		w := csv.NewWriter(f)
		err = w.Write([]string{"企業名", "index", "コード", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022"})
		if err != nil {
			return err
		}

		sem := semaphore.NewWeighted(concurrency)

		// read csv
		err = readCsv(sem, inputSrc, header, func(line int, companyName string) error {
			log.Printf("%d: %s\n", line, companyName)
			result, err := searchPastStock(companyName)
			if err != nil {
				return err
			}

			err = w.Write([]string{
				companyName,
				strconv.Itoa(line),
				result.StockCode,
				fmt.Sprintf("%.1f", result.Price2013),
				fmt.Sprintf("%.1f", result.Price2014),
				fmt.Sprintf("%.1f", result.Price2015),
				fmt.Sprintf("%.1f", result.Price2016),
				fmt.Sprintf("%.1f", result.Price2017),
				fmt.Sprintf("%.1f", result.Price2018),
				fmt.Sprintf("%.1f", result.Price2019),
				fmt.Sprintf("%.1f", result.Price2020),
				fmt.Sprintf("%.1f", result.Price2021),
				fmt.Sprintf("%.1f", result.Price2022)})

			if err != nil {
				return err
			}
			return nil
		})
		w.Flush()

		if err != nil {
			return err
		}
		return nil
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.scrape-nikkei-past-price.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().String("input", "", "入力用のcsvファイルのパスを指定してください")
	rootCmd.MarkFlagFilename("input", "csv")
	rootCmd.MarkFlagRequired("input")

	rootCmd.Flags().Int("header", 1, "ヘッダとして読み飛ばす行数を指定してください")

	rootCmd.Flags().String("output", "", "出力用のcsvファイルのパスを指定してください")
	rootCmd.MarkFlagRequired("output")

	rootCmd.Flags().Int64("concurrency", 5, "最大同時実行数を指定してください")
}
