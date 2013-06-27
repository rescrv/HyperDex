// C
#include <cassert>

// HyperDex
#include "common/regex_match.h"

static bool
anchored(const char* regex, const char* regex_end,
         const char* text,  const char* text_end);

static bool
starred(int c,
        const char* regex, const char* regex_end,
        const char* text,  const char* text_end);

bool
anchored(const char* regex, const char* regex_end,
         const char* text,  const char* text_end)
{
    assert(regex <= regex_end);
    assert(text <= text_end);

    if (regex == regex_end)
    {
        return true;
    }

    if (regex[0] == '\\')
    {
        if (regex + 1 < regex_end &&
            text < text_end &&
            regex[1] == text[0])
        {
            return anchored(regex + 2, regex_end, text + 1, text_end);
        }

        return false;
    }

    if (regex + 1 < regex_end && regex[1] == '*')
    {
        return starred(regex[0], regex + 2, regex_end, text, text_end);
    }

    if (regex[0] == '$' && regex + 1 == regex_end)
    {
        return text == text_end;
    }

    if (text < text_end && (regex[0] == '.' || regex[0] == text[0]))
    {
        return anchored(regex + 1, regex_end, text + 1, text_end);
    }

    return false;
}

bool
starred(int c,
        const char* regex, const char* regex_end,
        const char* text,  const char* text_end)
{
    for (; text <= text_end; ++text)
    {
        if (anchored(regex, regex_end, text, text_end))
        {
            return true;
        }

        if (text < text_end && *text != c && c != '.')
        {
            break;
        }
    }

    return false;
}

bool
hyperdex :: regex_match(const uint8_t* _regex, size_t regex_sz,
                        const uint8_t* _text, size_t text_sz)
{
    const char* regex = reinterpret_cast<const char*>(_regex);
    const char* text  = reinterpret_cast<const char*>(_text);
    const char* regex_end = regex + regex_sz;
    const char* text_end  = text + text_sz;

    if (regex_sz == 0)
    {
        return true;
    }

    if (regex[0] == '^')
    {
        return anchored(regex + 1, regex_end, text, text_end);
    }

    for (; text <= text_end; ++text)
    {
        if (anchored(regex, regex_end, text, text_end))
        {
            return true;
        }
    }

    return false;
}
