using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Prett.SqlServer
{
    public class Utils
    {
        public static string ReplaceSqlCommentsWithSpaces(string sourceCode)
        {
            int sourceCodeLength = sourceCode.Length;
            int lastCommentStart = sourceCodeLength - 2;

            char[] chars = new char[sourceCodeLength];
            sourceCode.CopyTo(0, chars, 0, sourceCodeLength);

            bool inComment = false;
            for (int c = 0; c < sourceCodeLength; c++)
            {
                if (c < lastCommentStart && chars[c] == '-' && chars[c + 1] == '-')
                {
                    inComment = true;
                }
                if (chars[c] == '\r' || chars[c] == '\n')
                {
                    inComment = false;
                }
                if (inComment)
                {
                    chars[c] = ' ';
                }
            }

            string ret = new string(chars);
            return ret;
        }

        public static string ByteArrayToString(byte[] byteArray)
        {
            if (!(byteArray is byte[]))
            {
                throw new ArgumentException("Not an object of byte[]");
            }

            byte[] bytes = (byte[])byteArray;
            StringBuilder sb = new StringBuilder();
            if (bytes.Length > 0)
            {
                sb.Append("0x");
            }
            foreach (byte b in bytes)
            {
                sb.AppendFormat("{0:x2}", b);
            }
            return sb.ToString().ToUpper();
        }

    }
}
